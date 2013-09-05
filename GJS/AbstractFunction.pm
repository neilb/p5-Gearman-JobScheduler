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
use Data::UUID;
use Sys::Path;
use File::Path qw(make_path);


use constant GJS_JOB_ID_MAX_LENGTH => 256;

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

=item * if C<progress_expected()> is enabled

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

Returns a number of retries each job will be attempted at. Returns 0 if the job
should not be retried.

=cut
requires 'retries';


=head2 (static) C<unique()>

Return true if the function is "unique".

Returns true if two or more jobs with the same parameters can not be run at the
same and instead should be merged into one.

=cut
requires 'unique';


=head2 (static) C<progress_expected()>

Return true if the function's jobs are expected to provide progress.

Returns true if the function's individual jobs are expected to provide progress
reports via C<$self-E<gt>set_progress($numerator, $denominator)>.

=cut
requires 'progress_expected';



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
		$gjs_job_id = _unique_job_id($function_name, $args, $gearman_job->handle());
	} else {
		$gjs_job_id = _unique_job_id($function_name, $args);
	}
	unless ($gjs_job_id) {
		die "Unable to determine unique GJS job ID";
	}

	my $log_path = $class->_init_and_return_worker_log_dir($function_name) . $gjs_job_id . '.log';
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

		# Try to run the job
		eval {
			my $instance = $class->new();

			# undef when running locally, instance when issued from _run_locally_from_gearman_worker
			$instance->_gearman_job($gearman_job);

			# Do the work
			$result = $instance->run($args);

			$instance->_gearman_job(undef);

			# Destroy instance
			$instance = undef;
		};
	    if ( $@ )
	    {
	        die "Job \"$gjs_job_id\" died: $@";
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
	say STDERR "Function name: $function_name";
	say STDERR "Unserialized args: " . Dumper($args);
	say STDERR "Serialized args: $args_serialized";

	# Gearman::XS::Client seems to not like undefined or empty workload()
	# so we pass 0 instead
	$args_serialized ||= 0;

	my ($ret, $result) = $client->do($function_name, $args_serialized);
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

	my ($ret, $task) = $client->add_task($function_name, $args_serialized);
	unless ($ret == GEARMAN_SUCCESS) {
		die "Gearman failed while adding task: " . $client->error();
	}

	$ret = $client->run_tasks();
	unless ($ret == GEARMAN_SUCCESS) {
		die "Gearman failed while running enqueued tasks: " . $client->error();
	}

	my $gearman_job_id = $task->job_handle();
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

# (static) Return an unique, path-safe job name which is suitable for writing
# to the filesystem
#
# Parameters:
# * Gearman function name, e.g. 'NinetyNineBottlesOfBeer'
# * hashref of job arguments, e.g. "{ 'how_many_bottles' => 13 }"
# * (optional) Gearman job ID, e.g.:
#     * "H:tundra.home:18" (as reported by an instance of Gearman::Job), or
#     * "127.0.0.1:4730//H:tundra.home:18" (as reported by gearmand)
#
# Returns: unique job ID, e.g.:
# * "084567C4146F11E38F00CB951DB7256D.NinetyNineBottlesOfBeer(how_many_bottles_=_2000)", or
# * "H_tundra.home_18.NinetyNineBottlesOfBeer(how_many_bottles_=_2000)"
sub _unique_job_id($$;$)
{
	my ($function_name, $job_args, $gearman_job_id) = @_;

	unless ($function_name) {
		return undef;
	}

	my $unique_id;
	if ($gearman_job_id) {

		# If Gearman job ID was passed as a parameter, this means that the job
		# was run by Gearman (by running run_on_gearman() or enqueue_on_gearman()).
		# Thus, the job has to be logged to a location that can later be found
		# by knowing the Gearman job ID.

		# Strip the host part (if present)
		if (index($gearman_job_id, '//') != -1) {
			my ($server, $internal_job_id) = split('//', $gearman_job_id);
			$gearman_job_id = $internal_job_id;
		}

		unless ($gearman_job_id =~ /^H:.+?:\d+?$/) {
			die "Invalid Gearman job ID: $gearman_job_id";
		}

		$unique_id = $gearman_job_id;

	} else {

		# If no Gearman job ID was provided, this means that the job is being
		# run locally.
		# The job's output still has to be logged somewhere, so we generate an
		# UUID to serve in place of Gearman job ID.

		my $ug    = new Data::UUID;
		my $uuid = $ug->create_str();	# e.g. "059303A4-F3F1-11E2-9246-FB1713B42706"
		$uuid =~ s/\-//gs;				# e.g. "059303A4F3F111E29246FB1713B42706"

		$unique_id = $uuid;		
	}


	# Convert to string
	$job_args = ($job_args and scalar keys $job_args)
		? join(', ', map { "$_ = $job_args->{$_}" } keys $job_args)
		: '';

	# UUID goes first in case the job name shortener decides to cut out a part of the job ID
	my $gjs_job_id = "$unique_id.$function_name($job_args)";
	if (length ($gjs_job_id) > GJS_JOB_ID_MAX_LENGTH) {
		$gjs_job_id = substr($gjs_job_id, 0, GJS_JOB_ID_MAX_LENGTH);
	}

	# Sanitize for paths
	$gjs_job_id =~ s/[^a-zA-Z0-9\.\-_\(\)=,]/_/gi;

	return $gjs_job_id;
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

# (static) Initialize (create missing directories) and return a worker log directory path (with trailing slash)
sub _init_and_return_worker_log_dir($$)
{
	my ($class, $function_name) = @_;

	if (ref $class) {
		die "Use this subroutine as a static method.";
	}

	my $config = GJS->_configuration;
	my $worker_log_dir = $config->{worker_log_dir} || Sys::Path->logdir . '/gjs/';

	# Add a trailing slash
    $worker_log_dir =~ s!/*$!/!;

    # Append the function name
    $worker_log_dir .= $function_name . '/';

    unless ( -d $worker_log_dir ) {
    	make_path( $worker_log_dir );
    }

    return $worker_log_dir;
}


1;

no Moose;    # gets rid of scaffolding

1;

=head1 TODO

=over 4

=item * improve differentiation between jobs, functions, tasks, etc.

=item * progress reports

=item * proper support of retrying (Perl's Gearman module doesn't support it)

=item * Figure out how to identify individual jobs (e.g.
add_default_feeds({media_id=1234})) on the web interface so that one can see
their status and completion.

=item * Email reports about failed function runs

=item * Script to run all workers at the same time

=item * (Maybe) Put the argument list as the first line of the log file
(argument list is truncated, sanitized and is there for the display purposes,
so maybe it wouldn't be that bad to leave it there.

=item * Make an infrastructure to query currently running jobs: e.g.
run_on_gearman returns some sort of an ID which is queryable through a helper
function to get the path of the log file and whatnot.

=item * test timeout

=item * test retries

=item * do the "unique" jobs still work?

=back
