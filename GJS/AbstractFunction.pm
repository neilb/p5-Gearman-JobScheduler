=head1 NAME

C<GJS::AbstractFunction> - An abstract class for a Gearman "function" which
is to be derived by working Gearman "functions".


=head1 LINGO

=over 4

=item * Gearman function

A function to be run by Gearman or locally, e.g. C<add_default_feeds>.

=item * Gearman job

An instance of the Gearman function doing the actual job with specific parameters.

=back

=cut
package GJS::AbstractFunction;

use strict;
use warnings;
use Modern::Perl "2012";

use Moose::Role;

require 'GJS.pm';
require 'GJS/ErrorLogTrapper.pm';

use Gearman::XS qw(:constants);
use Gearman::XS::Client;
use Gearman::XS::Task;
use Gearman::XS::Worker;

use IO::File;
use Capture::Tiny ':all';
use Time::HiRes;
use Data::Dumper;


# used for capturing STDOUT and STDERR output of each job and timestamping it;
# initialized before each job
use Log::Log4perl qw(:easy);



=head1 ABSTRACT INTERFACE

The following subroutines must be implemented by the subclasses of this class.

=head2 C<run($self, $args)>

Run the job.

Parameters:

=over 4

=item * C<$self>, a reference to the instance of the Gearman function class

=item * (optional) C<$args> (hashref), arguments needed for running the
Gearman function

=back

An instance (object) of the class will be created before each run. Class
instance variables (e.g. C<$self-E<gt>_my_variable>) will be discarded after
each run.

Returns result on success (serializable by the L<Storable> module). The result
will be discarded if the job is ordered on Gearman as a background process.

Provides progress reports when available:

=over 4

=item * by calling C<$self-E<gt>set_progress($numerator, $denominator)>

=back

C<die()>s on error.

Writes log to C<STDOUT> or C<STDERR> (preferably the latter).

=cut
requires 'run';


=head2 (static) C<job_timeout()>

Return the timeout of each job.

Returns the timeout (in seconds) of each job or 0 if there's no timeout.

=cut
requires 'job_timeout';


=head2 (static) C<retries()>

Return the number of retries for each job.

Returns a number of retries each job will be attempted at. For example, if the
number of retries is set to 3, the job will be attempted 4 four times in total.

Returns 0 if the job should not be retried (attempted only once).

=cut
requires 'retries';


=head2 (static) C<unique()>

Return true if the function is "unique" (only for Gearman requests).

Returns true if two or more jobs with the same parameters can not be run at the
same and instead should be merged into one.

=cut
requires 'unique';


=head1 HELPER SUBROUTINES

The following subroutines can be used by the deriving class.

=head2 C<$self-E<gt>set_progress($numerator, $denominator)>

Provide progress report while running the task (from C<run()>).

Examples:

=over 4

=item * C<$self-E<gt>set_progress(3, 10)>

3 out of 10 subtasks are complete.

=item * C<$self-E<gt>set_progress(45, 100)>

45 out of 100 subtasks are complete (or 45% complete).

=back

=cut
sub set_progress($$$)
{
	my ($self, $numerator, $denominator) = @_;

	unless (defined $self->_gearman_job) {
		# Running the job locally, Gearman doesn't have anything to do with this run
		DEBUG("Gearman job is undef");
		return;
	}
	unless ($denominator) {
		die "Denominator is 0.";
	}

	# Written to job's log
	say STDERR "$numerator/$denominator complete.";

	my $ret = $self->_gearman_job->send_status($numerator, $denominator);
	unless ($ret == GEARMAN_SUCCESS) {
		LOGDIE("Unable to send Gearman job status: " . $self->_gearman_job->error());
	}
}



=head1 CLIENT SUBROUTINES

The following subroutines can be used by "clients" in order to issue a Gearman
function.

=head2 (static) C<run_locally($args)>

Run locally and right away, blocking the parent process until it gets finished.

Parameters:

=over 4

=item * (optional) C<$args> (hashref), arguments required for running the
Gearman function  (serializable by the L<Storable> module)

=item * (optional, internal) instance of Gearman::XS::Job to be later used by
send_progress()

=back

Returns result (may be false of C<undef>) on success, C<die()>s on error

=cut
sub run_locally($;$$)
{
	my $class = shift;
	my $args = shift;
	my $gearman_job = shift;

	if (ref $class) {
		die "Use this subroutine as a static method, e.g. MyGearmanFunction->run_locally()";
	}

	# say STDERR "Running locally";

	if ((@_) or
		($args and ref($args) ne 'HASH' ) or
		(defined $gearman_job and ref($gearman_job) ne 'Gearman::XS::Job')) {

		die "run() should accept a single hashref for all the arguments.";
	}

	my $function_name = $class->_function_name();
	my $gjs_job_id;
	if ($gearman_job) {
		# Running from Gearman
		unless (defined $gearman_job->handle()) {
			die "Unable to find a Gearman job ID to be used for logging";
		}
		$gjs_job_id = GJS->_unique_path_job_id($function_name, $args, $gearman_job->handle());
	} else {
		$gjs_job_id = GJS->_unique_path_job_id($function_name, $args);
	}
	unless ($gjs_job_id) {
		die "Unable to determine unique GJS job ID";
	}

	my $log_path = GJS->_init_and_return_worker_log_dir($function_name) . $gjs_job_id . '.log';
	my $starting_job_message;
	if ( -f $log_path ) {
		# Worker crashed last time and now tries to write to the same log path
		# (will append to the log)
		$starting_job_message = "Restarting job ID \"$gjs_job_id\", logging to \"$log_path\" ...";
	} else {
		$starting_job_message = "Starting job ID \"$gjs_job_id\", logging to \"$log_path\" ...";
	}

	my $finished_job_message;

	_reset_log4perl();
	INFO($starting_job_message);

	Log::Log4perl->easy_init({
		level => $DEBUG,
		utf8=>1,
		file => ">>$log_path",	# do not use STDERR / STDOUT here because it would end up with recursion
		layout => "%d{ISO8601} [%P]: %m"
	});


	# Tie STDOUT / STDERR to Log4perl handler
	tie *STDOUT, "GJS::ErrorLogTrapper";
	tie *STDERR, "GJS::ErrorLogTrapper";

	my $result;

	eval {

		say STDERR $starting_job_message;
		say STDERR "========";
		say STDERR "";

		my $start = Time::HiRes::gettimeofday();

	    my $job_succeeded = 0;
	    for ( my $retry = 0 ; $retry <= $class->retries() ; ++$retry )
	    {
	        if ( $retry > 0 )
	        {
	        	say STDERR "";
				say STDERR "========";
	            say STDERR "Retrying ($retry)...";
				say STDERR "========";
	        	say STDERR "";
	        }

	        eval {

				# Try to run the job
				my $instance = $class->new();

				# _gearman_job is undef when running locally, instance when issued from _run_locally_from_gearman_worker
				$instance->_gearman_job($gearman_job);

				# Do the work
				$result = $instance->run($args);

				# Unset the _gearman_job for the sake of cleanliness
				$instance->_gearman_job(undef);

				# Destroy instance
				$instance = undef;

	            $job_succeeded = 1;
	        };

	        if ( $@ )
	        {
	            say STDERR "Job \"$gjs_job_id\" failed: $@";
	        }
	        else
	        {
	            last;
	        }
	    }

	    unless ( $job_succeeded )
	    {
	    	my $job_failed_message = "Job \"$gjs_job_id\" failed" . ($class->retries() ? " after " . $class->retries() . " retries" : "") . ": $@";

			say STDERR "";
			say STDERR "========";
	    	say STDERR $job_failed_message;
	        die $job_failed_message;
	    }

	    my $end = Time::HiRes::gettimeofday();

		say STDERR "";
		say STDERR "========";
		$finished_job_message = "Finished job ID \"$gjs_job_id\" in " . sprintf("%.2f", $end - $start) . " seconds";
	    say STDERR $finished_job_message;

	};

	my $error = $@;
	if ($error) {
		# Write to job's log
		say STDERR "Job died: $error";
	}

	# Untie STDOUT / STDERR from Log4perl
    untie *STDERR;
    untie *STDOUT;

	_reset_log4perl();
	if ($finished_job_message) {
		INFO($finished_job_message);
	}

    if ( $error )
    {
    	# Print out to worker's STDERR and die()
    	LOGDIE("$error");
    }

	return $result;
}


=head2 (static) C<run_on_gearman($args)>

Run on Gearman, wait for the task to complete, return the result; block the
process until the job is complete.

Parameters:

=over 4

=item * (optional) C<$args> (hashref), arguments needed for running the Gearman
function (serializable by the L<Storable> module)

=back

Returns result (may be false of C<undef>) on success, C<die()>s on error

=cut
sub run_on_gearman($;$)
{
	my $class = shift;
	my $args = shift;

	if (ref $class) {
		die "Use this subroutine as a static method, e.g. MyGearmanFunction->run_on_gearman()";
	}

	my $config = GJS->_configuration;
	my $client = GJS->_gearman_xs_client;
	my $function_name = $class->_function_name;
	unless ($function_name) {
		die "Unable to determine function name.";
	}

	# Run
	my $args_serialized = GJS->_serialize_hashref($args);

	# Gearman::XS::Client seems to not like undefined or empty workload()
	# so we pass 0 instead
	$args_serialized ||= 0;

	# Do the job
	my ($ret, $result);
	if ($class->unique()) {
		# If the job is set to be "unique", we need to pass a "unique identifier"
		# to Gearman so that it knows which jobs to merge into one
		($ret, $result) = $client->do($function_name, $args_serialized, GJS->_unique_job_id($function_name, $args));
	} else {
		($ret, $result) = $client->do($function_name, $args_serialized);
	}
	unless ($ret == GEARMAN_SUCCESS) {
		die "Gearman failed: " . $client->error();
	}

	# Deserialize the results (because they were serialized and put into
	# hashref by _run_locally_from_gearman_worker())
	my $result_deserialized = GJS->_unserialize_hashref($result);
	if (ref $result_deserialized eq 'HASH') {
		return $result_deserialized->{result};
	} else {
		# No result
		return undef;
	}
}


=head2 (static) C<enqueue_on_gearman($args)>

Enqueue on Gearman, do not wait for the task to complete, return immediately;
do not block the parent process until the job is complete.

Parameters:

=over 4

=item * (optional) C<$args> (hashref), arguments needed for running the Gearman
function (serializable by the L<Storable> module)

=back

Returns Gearman-provided string job identifier (Gearman job ID) if the job was
enqueued successfully, C<die()>s on error.

=cut
sub enqueue_on_gearman($;$)
{
	my $class = shift;
	my $args = shift;

	if (ref $class) {
		die "Use this subroutine as a static method, e.g. MyGearmanFunction->enqueue_on_gearman()";
	}

	my $config = GJS->_configuration;
	my $client = GJS->_gearman_xs_client;
	my $function_name = $class->_function_name;
	unless ($function_name) {
		die "Unable to determine function name.";
	}

	# Add task
	my $args_serialized = GJS->_serialize_hashref($args);

	# Gearman::XS::Client seems to not like undefined or empty workload()
	# so we pass 0 instead
	$args_serialized ||= 0;

	my ($ret, $gearman_job_id);
	if ($class->unique()) {
		# If the job is set to be "unique", we need to pass a "unique identifier"
		# to Gearman so that it knows which jobs to merge into one
		($ret, $gearman_job_id) = $client->do_background($function_name, $args_serialized, GJS->_unique_job_id($function_name, $args));
	} else {
		($ret, $gearman_job_id) = $client->do_background($function_name, $args_serialized);
	}
	unless ($ret == GEARMAN_SUCCESS) {
		die "Gearman failed while doing task in background: " . $client->error();
	}

	say STDERR "Enqueued job '$gearman_job_id' on Gearman";

	return $gearman_job_id;
}


# _run_locally_from_gearman_worker() will pass this parameter to run_locally()
# which, in turn, will temporarily place an instance of Gearman::XS::Job to
# this variable so that set_progress() helper can later use it
has '_gearman_job' => ( is => 'rw' );


# Run locally and right away, blocking the parent process while it gets finished
# (issued by the Gearman worker)
# Returns result (may be false of undef) on success, die()s on error
sub _run_locally_from_gearman_worker($;$)
{
	my $class = shift;
	my $gearman_job = shift;

	if (ref $class) {
		LOGDIE("Use this subroutine as a static method.");
	}

	# Args were serialized by run_on_gearman()
	my $args = GJS->_unserialize_hashref($gearman_job->workload());

	my $result;
	eval {
		$result = $class->run_locally($args, $gearman_job);
	};
	if ($@) {
		LOGDIE("Gearman job died: $@");
	}

	# Create a hashref and serialize result because it's going to be passed over Gearman
	$result = { 'result' => $result };
	my $result_serialized = GJS->_serialize_hashref($result);

	return $result_serialized;
}


# Returns function name (e.g. 'NinetyNineBottlesOfBeer')
sub _function_name($)
{
	my $self_or_class = shift;

	my $function_name = '';
	if (ref($self_or_class)) {
		# Instance
		$function_name = '' . ref($self_or_class);
	} else {
		# Static
		$function_name = $self_or_class;
	}

	if ($function_name eq 'AbstractFunction') {
		die "Unable to determine function name.";
	}

	return $function_name;
}

# (static) Reset Log::Log4perl to write to the STDERR / STDOUT and not to file
sub _reset_log4perl()
{
	Log::Log4perl->easy_init({
		level => $DEBUG,
		utf8=>1,
		layout => "%d{ISO8601} [%P]: %m%n"
	});
}


1;

no Moose;    # gets rid of scaffolding

1;

=head1 TODO

=over 4

=item * Email reports about failed function runs

=item * Script to run all workers at the same time

=item * (Maybe) Put the argument list as the first line of the log file
(argument list is truncated, sanitized and is there for the display purposes,
so maybe it wouldn't be that bad to leave it there.

=item * test timeout

=item * job priorities

=item * store Gearman queue in PostgreSQL?

=back
