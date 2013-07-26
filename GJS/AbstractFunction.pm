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

use GJS::ErrorLogTrapper;

use IO::File;
use Capture::Tiny ':all';
use Time::HiRes;
use Data::Dumper;
use Data::UUID;
use Gearman::Client;
use Gearman::Task;
use Gearman::Worker;
use YAML qw(LoadFile);
use Storable qw(freeze thaw);
use Data::Compare;
use Sys::Path;
use File::Path qw(make_path);

use constant GJS_CONFIG_FILE => 'config.yml';
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

=item * by calling C<$self-E<gt>progress($numerator, $denominator)>

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
reports via C<$self-E<gt>progress($numerator, $denominator)>.

=cut
requires 'progress_expected';



=head1 HELPER SUBROUTINES

The following subroutines can be used by the deriving class.

=head2 C<$self-E<gt>progress($numerator, $denominator)>

Provide progress report while running the task (from C<run()>).

Examples:

=over 4

=item * C<$self-E<gt>progress(3, 10)>

3 out of 10 subtasks are complete.

=item * C<$self-E<gt>progress(45, 100)>

45 out of 100 subtasks are complete (or 45% complete).

=back

=cut
sub progress($$$)
{
	my ($self, $numerator, $denominator) = @_;

	unless (defined $self->_gearman_worker) {
		# Running the job locally, Gearman doesn't have anything to do with this run
		return;
	}
	unless ($denominator) {
		die "Denominator is 0.";
	}

	say STDERR "$numerator/$denominator complete.";

	$self->_gearman_worker->set_status($numerator, $denominator);
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

=back

Returns result (may be false of C<undef>) on success, C<die()>s on error

=cut
sub run_locally($;$$)
{
	my $class = shift;
	my $args = shift;
	my $gearman_worker = shift;

	if (ref $class) {
		die "Use this subroutine as a static method, e.g. MyGearmanFunction->run_locally()";
	}

	# say STDERR "Running locally";

	if (@_ or($args and ref($args) ne 'HASH' ) or (defined $gearman_worker and ref($gearman_worker) ne 'Gearman::Job' and ref($gearman_worker) ne 'Gearman::Worker')) {
		die "run() should accept a single hashref for all the arguments.";
	}

	my $function_name = $class->_function_name();
	my $job_id = _unique_job_id($function_name, $args);
	unless ($job_id) {
		die "Unable to determine unique job ID";
	}

	my $log_path = $class->_init_and_return_worker_log_dir($function_name) . $job_id . '.log';
	if ( -f $log_path ) {
		die "Worker log already exists at path '$log_path'.";
	}

	my $starting_job_message = "Starting job ID \"$job_id\", logging to \"$log_path\" ...";
	my $finished_job_message;

	_reset_log4perl();
	INFO($starting_job_message);

	Log::Log4perl->easy_init({
		level => $DEBUG,
		utf8=>1,
		file => $log_path,	# do not use STDERR / STDOUT here because it would end up with recursion
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
			$instance->_gearman_worker($gearman_worker);

			# Do the work
			$result = $instance->run($args);

			# Destroy instance
			$instance = undef;
		};
	    if ( $@ )
	    {
	        die "Job died: $@";
	    }

	    my $end = Time::HiRes::gettimeofday();

		say STDERR "";
		say STDERR "========";
		$finished_job_message = "Finished job ID \"$job_id\" in " . sprintf("%.2f", $end - $start) . " seconds";
	    say STDERR $finished_job_message;

	};

	my $error = $@;

	# Untie STDOUT / STDERR from Log4perl
    untie *STDERR;
    untie *STDOUT;

	_reset_log4perl();
	INFO($finished_job_message);

    if ( $error )
    {
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

	my $config = $class->_configuration;

	my $client = Gearman::Client->new;
	$client->job_servers(@{$config->{servers}});

	my $task = $class->_gearman_task_from_args($config, $args);
	my $result_ref = $client->do_task($task);
    # say STDERR "Serialized result: " . Dumper($result_ref);

	my $result_deserialized = undef;

	if (defined $result_ref) {
		$result_deserialized = thaw($$result_ref);
		$result_deserialized = $$result_deserialized;
	}

	return $result_deserialized;
}


=head2 (static) C<enqueue_on_gearman($args)>

Enqueue on Gearman, do not wait for the task to complete, return immediately;
do not block the parent process until the job is complete.

Parameters:

=over 4

=item * (optional) C<$args> (hashref), arguments needed for running the Gearman
function (serializable by the L<Storable> module)

=back

Returns Gearman-provided string job identifier if the job was enqueued
successfully, C<die()>s on error.

=cut
sub enqueue_on_gearman($;$)
{
	my $class = shift;
	my $args = shift;

	if (ref $class) {
		die "Use this subroutine as a static method, e.g. MyGearmanFunction->enqueue_on_gearman()";
	}

	my $config = $class->_configuration;

	my $client = Gearman::Client->new;
	$client->job_servers(@{$config->{servers}});

	my $task = $class->_gearman_task_from_args($config, $args);
	my $job_id = $client->dispatch_background($task);
    
	return $job_id;
}


# (static) Return configuration, die() on error
sub _configuration($)
{
	my $class = shift;

	my $config = LoadFile(GJS_CONFIG_FILE) or LOGDIE("Unable to read configuration from '" . GJS_CONFIG_FILE . "': $!");
	unless (scalar (@{$config->{servers}})) {
		die "No servers are configured.";
	}

	return $config;	
}


# (static) Validate the job arguments, create Gearman task from parameters or die on error
sub _gearman_task_from_args($$;$)
{
	my $class = shift;
	my $config = shift;
	my $args = shift;

	if (ref $class) {
		die "Use this subroutine as a static method.";
	}

	if (@_ or ($args and ref($args) ne 'HASH' )) {
		die "run() should accept arguments as a hashref";
	}

	my $function_name = $class->_function_name;
	unless ($function_name) {
		die "Unable to determine function name.";
	}

	# Gearman accepts only scalar arguments
	my $args_serialized = undef;
	eval {
		# say STDERR "Arguments: " . Dumper($args);
		$args_serialized = freeze \%{$args};
		# say STDERR "Serialized arguments: " . Dumper($args_serialized);
		my $args_deserialized = \%{ thaw($args_serialized) };
		# say STDERR "Deserialized arguments: " . Dumper($args_deserialized);
		unless (Compare($args, $args_deserialized)) {
			die "Serialized and deserialized argument hashes differ.";
		}
	};
	if ($@)
	{
		die "Unable to serialize the argument hash with the Storable module because: $@";
	}

	my $task = Gearman::Task->new($function_name, \$args_serialized, {
		uniq => $class->unique,
		on_complete => sub { say STDERR "Complete!" },
		on_fail => sub { say STDERR "Failed for the last time" },
		on_retry => sub { say STDERR "Retry" },
		on_status => sub { say STDERR "Status" },
		retry_count => $class->retries,
		try_timeout => $class->job_timeout,
	});

	return $task;
}


# _run_locally_from_gearman_worker() will pass this parameter to run_locally()
# which in turn will temporarily place a Gearman worker to this variable so
# that progress() helper can use it
has '_gearman_worker' => ( is => 'rw' );


# Run locally and right away, blocking the parent process while it gets finished
# (issued either by the Gearman worker)
# Returns result (may be false of undef) on success, die()s on error
sub _run_locally_from_gearman_worker($;$)
{
	my $class = shift;
	my $gearman_worker = shift;

	if (ref $class) {
		die "Use this subroutine as a static method.";
	}

	# Arguments are thawed
	my $args_deserialized = \%{ thaw($gearman_worker->arg) };

	my $result;
	eval {
		$result = $class->run_locally($args_deserialized, $gearman_worker);
	};
	if ($@) {
		LOGDIE("$@");
	}

	# Serialize result because it's going to be passed over Gearman
	# say STDERR "Unserialized result: " . Dumper($result);
	my $result_serialized = freeze \$result;
	# say STDERR "Serialized result: " . Dumper($result_serialized);
	my $result_deserialized = thaw($result_serialized);
	$result_deserialized = $$result_deserialized;
	# say STDERR "Deserialized result: " . Dumper($result_deserialized);
	unless (Compare($result, $result_deserialized)) {
		die "Serialized and deserialized results differ.";
	}

	return $result_serialized;
}

# (static) Return an unique, safe job name which is suitable for writing to the filesystem
sub _unique_job_id($$)
{
	my ($function_name, $job_args) = @_;

	my $ug    = new Data::UUID;
	my $uuid = $ug->create_str();	# e.g. 059303A4-F3F1-11E2-9246-FB1713B42706
	$uuid =~ s/\-//gs;				# e.g. 059303A4F3F111E29246FB1713B42706

	unless ($function_name) {
		return undef;
	}

	# Convert to string
	$job_args = ($job_args and scalar keys $job_args)
		? join(', ', map { "$_ = $job_args->{$_}" } keys $job_args)
		: '';

	# UUID goes first in case the job name shortener decides to cut out a part of the job ID
	my $job_id = "$uuid.$function_name($job_args)";
	if (length ($job_id) > GJS_JOB_ID_MAX_LENGTH) {
		$job_id = substr($job_id, 0, GJS_JOB_ID_MAX_LENGTH);
	}

	# Sanitize path
	$job_id =~ s/[^a-zA-Z0-9\.\-_\(\)=,]/_/gi;

	return $job_id;
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

	my $config = $class->_configuration;
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

=back
