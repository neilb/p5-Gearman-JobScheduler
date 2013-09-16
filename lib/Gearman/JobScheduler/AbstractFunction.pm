=head1 NAME

C<Gearman::JobScheduler::AbstractFunction> - An abstract class for a Gearman
"function" which is to be derived by working Gearman "functions".


=head1 LINGO

=over 4

=item * Gearman function

A function to be run by Gearman or locally, e.g. C<add_default_feeds>.

=item * Gearman job

An instance of the Gearman function doing the actual job with specific parameters.

=back

=cut
package Gearman::JobScheduler::AbstractFunction;

use strict;
use warnings;
use Modern::Perl "2012";
use feature qw(switch);

use Moose::Role;

use Gearman::JobScheduler;	# helper subroutines
use Gearman::JobScheduler::Configuration;
use Gearman::JobScheduler::ErrorLogTrapper;

use Gearman::XS qw(:constants);
use Gearman::XS::Client;
use Gearman::XS::Task;
use Gearman::XS::Worker;

use IO::File;
use Capture::Tiny ':all';
use Time::HiRes;
use Data::Dumper;
use DateTime;
use File::ReadBackwards;
use Readonly;
use Sys::Hostname;

# used for capturing STDOUT and STDERR output of each job and timestamping it;
# initialized before each job
use Log::Log4perl qw(:easy);


=head1 ABSTRACT INTERFACE

The following subroutines must be implemented by the subclasses of this class.

=head2 REQUIRED

=head3 C<run($self, $args)>

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


=head2 OPTIONAL

=head3 (static) C<timeout()>

Return the timeout of each job.

Returns the timeout (in seconds) of each job or 0 if there's no timeout.

Default implementation of this subroutine returns 0 (no timeout).

=cut
sub timeout()
{
	# By default, individual job never times out
	return 0;
}


=head3 (static) C<retries()>

Return the number of retries for each job.

Returns a number of retries each job will be attempted at. For example, if the
number of retries is set to 3, the job will be attempted 4 four times in total.

Returns 0 if the job should not be retried (attempted only once).

Default implementation of this subroutine returns 0 (no retries).

=cut
sub retries()
{
	# By default the job will not be retried if it fails
	return 0;
}


=head3 (static) C<unique()>

Return true if the function is "unique" (only for Gearman requests).

Returns true if two or more jobs with the same parameters can not be run at the
same and instead should be merged into one.

Default implementation of this subroutine returns "true".

=cut
sub unique()
{
	# By default the jobs are "unique", e.g. if there's already an
	# "Addition({operand_a => 2, operand_b => 3})" job running, a new one won't
	# be initialized
	return 1;
}


=head3 (static) C<notify_on_failure()>

Return true if the client / worker should send error report by email when the function fails.

Returns true if the GJS client (in case C<run_locally()> is used) or worker
(in case C<run_on_gearman()> or C<enqueue_on_gearman()> is being used) should
send an email when the function fails to run.

Default implementation of this subroutine returns "true".

=cut
sub notify_on_failure()
{
	# By default jobs will send notifications when they fail
	return 1;
}


=head3 (static) C<priority()>

Return priority of the job ("low", "normal" or "high"). This will influence
Gearman's queueing mechanism and prioritize "high priority" jobs.

Returns one of the three constants:

=over 4

=item * C<GJS_JOB_PRIORITY_LOW()>, if the job is considered of "low priority".

=item * C<GJS_JOB_PRIORITY_NORMAL()> if the job is considered of "normal priority".

=item * C<GJS_JOB_PRIORITY_HIGH()> if the job is considered of "high priority".

=back

Default implementation of this subroutine returns C<GJS_JOB_PRIORITY_NORMAL()>
("normal priority" job).

=cut

# Gearman job priorities (subroutines instead of constants because exporting
# constants with Moose in place is painful)
sub GJS_JOB_PRIORITY_LOW { 'low' }
sub GJS_JOB_PRIORITY_NORMAL { 'normal' }
sub GJS_JOB_PRIORITY_HIGH { 'high' }

sub priority()
{
	return GJS_JOB_PRIORITY_NORMAL();
}


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

=head2 (static) C<$class-E<gt>run_locally([$args, $config])>

Run locally and right away, blocking the parent process until it gets finished.

Parameters:

=over 4

=item * (optional) C<$args> (hashref), arguments required for running the
Gearman function  (serializable by the L<Storable> module)

=item * (optional) instance of Gearman::JobScheduler::Configuration to be used by the worker

=item * (optional, internal) instance of Gearman::XS::Job to be later used by
send_progress()

=back

Returns result (may be false of C<undef>) on success, C<die()>s on error

=cut
sub run_locally($;$$$)
{
	my $class = shift;
	my $args = shift;
	my $config = shift;
	my $gearman_job = shift;

	if (ref $class) {
		die "Use this subroutine as a static method, e.g. MyGearmanFunction->run_locally()";
	}

	unless ($config) {
		$config = Gearman::JobScheduler::_default_configuration();
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
		$gjs_job_id = Gearman::JobScheduler::_unique_path_job_id($function_name, $args, $gearman_job->handle());
	} else {
		$gjs_job_id = Gearman::JobScheduler::_unique_path_job_id($function_name, $args);
	}
	unless ($gjs_job_id) {
		die "Unable to determine unique GJS job ID";
	}

	my $log_path = Gearman::JobScheduler::_init_and_return_worker_log_dir($function_name, $config) . $gjs_job_id . '.log';
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
	tie *STDOUT, "Gearman::JobScheduler::ErrorLogTrapper";
	tie *STDERR, "Gearman::JobScheduler::ErrorLogTrapper";

	my $result;

	eval {

		say STDERR $starting_job_message;
		say STDERR "========";
		say STDERR "Arguments: " . Dumper($args);
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
    	# Send email notification (if needed)
    	eval {
	    	if ($class->notify_on_failure()) {

	    		my $now = DateTime->now()->strftime('%a, %d %b %Y %H:%M:%S %z');
	    		my $hostname = hostname;

	    		# Tail the log file
	    		my Readonly $how_many_lines = 50;
	    		my $last_lines = '';
	    		my $lines_read;
	    		my $bw = File::ReadBackwards->new( $log_path ) or die "Unable to open '$log_path' for tailing: $!";
	    		for ($lines_read = 1; $lines_read <= $how_many_lines; ++$lines_read) {
	    			my $log_line = $bw->readline;
	    			if (defined $log_line) {
	    				$last_lines = "$log_line$last_lines";
	    			} else {
	    				last;
	    			}
	    		}

	    		my $message_subject = 'Function "' . $function_name . '" failed';
	    		my $message_body = <<EOF;
Gearman function "$function_name" failed while running on "$hostname" at $now because:

<snip>
$error
</snip>

Location of the log: $log_path

Last $lines_read lines of the log:

<snip>
$last_lines
</snip>
EOF
	    		Gearman::JobScheduler::_send_email($message_subject, $message_body, $config);
	    	}
	    };
	    if ($@) {
	    	$error = "Failed to send notification email informing about the job failure: $@\nJob failed because: $error";
	    }

    	# Print out to worker's STDERR and die()
    	LOGDIE("$error");
    }

	return $result;
}


=head2 (static) C<$class-E<gt>run_on_gearman([$args, $config])>

Run on Gearman, wait for the task to complete, return the result; block the
process until the job is complete.

Parameters:

=over 4

=item * (optional) C<$args> (hashref), arguments needed for running the Gearman
function (serializable by the L<Storable> module)

=item * (optional) Instance of Gearman::JobScheduler::Configuration to be used by the client.

=back

Returns result (may be false of C<undef>) on success, C<die()>s on error

=cut
sub run_on_gearman($;$$)
{
	my $class = shift;
	my $args = shift;
	my $config = shift;

	if (ref $class) {
		die "Use this subroutine as a static method, e.g. MyGearmanFunction->run_on_gearman()";
	}

	unless ($config) {
		$config = Gearman::JobScheduler::_default_configuration();
	}

	my $client = Gearman::JobScheduler::_gearman_xs_client($config);
	my $function_name = $class->_function_name;
	unless ($function_name) {
		die "Unable to determine function name.";
	}

	# Run
	my $args_serialized = Gearman::JobScheduler::_serialize_hashref($args);

	# Gearman::XS::Client seems to not like undefined or empty workload()
	# so we pass 0 instead
	$args_serialized ||= 0;

	# Choose the client subroutine to use (based on the priority)
	my $client_do_ref = undef;
	given ($class->priority()) {
		when(GJS_JOB_PRIORITY_LOW()) { $client_do_ref = sub { $client->do_low(@_) }; }
		when(GJS_JOB_PRIORITY_NORMAL()) { $client_do_ref = sub { $client->do(@_) }; }
		when(GJS_JOB_PRIORITY_HIGH()) { $client_do_ref = sub { $client->do_high(@_) }; }
		default { die "Unknown job priority: " . $class->priority() }
	}

	# Client arguments
	my @client_args;
	if ($class->unique()) {
		# If the job is set to be "unique", we need to pass a "unique identifier"
		# to Gearman so that it knows which jobs to merge into one
		@client_args = ($function_name, $args_serialized, Gearman::JobScheduler::_unique_job_id($function_name, $args));
	} else {
		@client_args = ($function_name, $args_serialized);
	}

	# Do the job
	my ($ret, $result) = &{$client_do_ref}(@client_args);
	unless ($ret == GEARMAN_SUCCESS) {
		die "Gearman failed: " . $client->error();
	}

	# Deserialize the results (because they were serialized and put into
	# hashref by _run_locally_from_gearman_worker())
	my $result_deserialized = Gearman::JobScheduler::_unserialize_hashref($result);
	if (ref $result_deserialized eq 'HASH') {
		return $result_deserialized->{result};
	} else {
		# No result
		return undef;
	}
}


=head2 (static) C<$class-E<gt>enqueue_on_gearman([$args, $config])>

Enqueue on Gearman, do not wait for the task to complete, return immediately;
do not block the parent process until the job is complete.

Parameters:

=over 4

=item * (optional) C<$args> (hashref), arguments needed for running the Gearman
function (serializable by the L<Storable> module)

=item * (optional) Instance of Gearman::JobScheduler::Configuration to be used by the client.

=back

Returns Gearman-provided string job identifier (Gearman job ID) if the job was
enqueued successfully, C<die()>s on error.

=cut
sub enqueue_on_gearman($;$$)
{
	my $class = shift;
	my $args = shift;
	my $config = shift;

	if (ref $class) {
		die "Use this subroutine as a static method, e.g. MyGearmanFunction->enqueue_on_gearman()";
	}

	unless ($config) {
		$config = Gearman::JobScheduler::_default_configuration();
	}

	my $client = Gearman::JobScheduler::_gearman_xs_client($config);
	my $function_name = $class->_function_name;
	unless ($function_name) {
		die "Unable to determine function name.";
	}

	# Add task
	my $args_serialized = Gearman::JobScheduler::_serialize_hashref($args);

	# Gearman::XS::Client seems to not like undefined or empty workload()
	# so we pass 0 instead
	$args_serialized ||= 0;

	# Choose the client subroutine to use (based on the priority)
	my $client_do_bg_ref = undef;
	given ($class->priority()) {
		when(GJS_JOB_PRIORITY_LOW()) { $client_do_bg_ref = sub { $client->do_low_background(@_) }; }
		when(GJS_JOB_PRIORITY_NORMAL()) { $client_do_bg_ref = sub { $client->do_background(@_) }; }
		when(GJS_JOB_PRIORITY_HIGH()) { $client_do_bg_ref = sub { $client->do_high_background(@_) }; }
		default { die "Unknown job priority: " . $class->priority() }
	}

	# Client arguments
	my @client_args;
	if ($class->unique()) {
		# If the job is set to be "unique", we need to pass a "unique identifier"
		# to Gearman so that it knows which jobs to merge into one
		@client_args = ($function_name, $args_serialized, Gearman::JobScheduler::_unique_job_id($function_name, $args));
	} else {
		@client_args = ($function_name, $args_serialized);
	}

	# Enqueue the job
	my ($ret, $gearman_job_id) = &{$client_do_bg_ref}(@client_args);
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
sub _run_locally_from_gearman_worker($$;$)
{
	my ($class, $config, $gearman_job) = @_;

	if (ref $class) {
		LOGDIE("Use this subroutine as a static method.");
	}

	# Args were serialized by run_on_gearman()
	my $args = Gearman::JobScheduler::_unserialize_hashref($gearman_job->workload());

	my $result;
	eval {
		$result = $class->run_locally($args, $config, $gearman_job);
	};
	if ($@) {
		LOGDIE("Gearman job died: $@");
	}

	# Create a hashref and serialize result because it's going to be passed over Gearman
	$result = { 'result' => $result };
	my $result_serialized = Gearman::JobScheduler::_serialize_hashref($result);

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


no Moose;    # gets rid of scaffolding

1;

=head1 TODO

=over 4

=item * code formatting

=item * test timeout

=item * store Gearman queue in PostgreSQL?

=item * unit tests

=item * remove the requirement to pass a function name parameter to log_path_for_gearman_job()

=back
