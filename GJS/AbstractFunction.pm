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


#
# ABSTRACT INTERFACE
# ==================
#

# Run the job
# -----------
#
# The job:
# * accepts two parameters:
#     * $self as the first parameter
#     * $args (hashref) as the second parameter
# * does not use class instance variables because their behavior is undefined
# * returns result on success (serializable by the Storable module)
#     * the result will be discarded if the job is ordered on Gearman as a background process
# * provides progress reports when available:
#     * if progress_expected() is enabled
#     * by calling $self->progress($numerator, $denominator)
# * die()s on error
# * writes log to STDOUT or STDERR (preferably the latter)
requires 'run';


# (static) Return the timeout of each job
# ---------------------------------------
#
# Returns the timeout (in seconds) of each job or 0 if there's no timeout.
requires 'job_timeout';


# (static) Return the number of retries for each job
# --------------------------------------------------
#
# Returns a number of retries each job will be attempted at.
# Return 0 if the job should not be retried.
requires 'retries';


# (static) Return true if the function is "unique"
# ------------------------------------------------
#
# Returns true if two or more jobs with the same parameters can not be run at
# the same and instead should be merged into one.
requires 'unique';


# (static) Return true if the function's jobs are expected to provide progress
# ----------------------------------------------------------------------------
#
# Returns true if the function's individual jobs are expected to provide
# progress reports via $self->progress($numerator, $denominator).
requires 'progress_expected';



#
# =========================
# END OF ABSTRACT INTERFACE
#


#
# HELPERS
# =======
#

# Provide progress report while running the task
# ----------------------------------------------
#
# Params:
# * $self
# * numerator
# * denominator
#
# Examples:
# $self->progress(3, 10) -- 3 out of 10 subtasks are complete
# $self->progress(45, 100) -- 45 out of 100 subtasks are complete (or 45% complete)
sub progress($$$)
{
	my ($self, $numerator, $denominator) = @_;

	unless ($self->_gearman_worker) {
		die "Gearman worker is not defined.";
	}
	unless ($denominator) {
		die "Denominator is 0.";
	}

	say STDERR "$numerator/$denominator complete.";

	$self->_gearman_worker->set_status($numerator, $denominator);
}

#
# ==============
# END OF HELPERS
#


# Run locally and right away, blocking the parent process while it gets finished
# (issued either by the original caller or the Gearman worker)
# Returns result (may be false of undef) on success, die()s on error
sub run_locally($;$)
{
	my $self = shift;
	my $args = shift;

	# say STDERR "Running locally";

	if (@_ or ($args and ref($args) ne 'HASH' )) {
		die "run() should accept arguments as a hashref";
	}

	my $function_name = $self->_function_name();
	my $job_id = _unique_job_id($function_name, $args);
	unless ($job_id) {
		die "Unable to determine unique job ID";
	}

	my $log_path = $self->_init_and_return_worker_log_dir . $job_id . '.log';
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
			$result = $self->run($args);
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

# Run on Gearman, wait for the task to complete, return the result;
# block the process until the job is complete
# (issued by the original caller)
# Returns job's result on success, die()s on error
sub run_on_gearman($;$)
{
	my $self = shift;
	my $args = shift;

	my $config = $self->_configuration;

	my $client = Gearman::Client->new;
	$client->job_servers(@{$config->{servers}});

	my $task = $self->_gearman_task_from_args($config, $args);
	my $result_ref = $client->do_task($task);
    # say STDERR "Serialized result: " . Dumper($result_ref);

	my $result_deserialized = undef;

	if (defined $result_ref) {
		$result_deserialized = thaw($$result_ref);
		$result_deserialized = $$result_deserialized;
	}

	return $result_deserialized;
}


# Enqueue on Gearman, do not wait for the task to complete, return immediately;
# do not block the process until the job is complete
# (issued by the original caller)
# Returns Gearman-provided job identifier if job was enqueued successfully, die()s on error
sub enqueue_on_gearman($;$)
{
	my $self = shift;
	my $args = shift;

	my $config = $self->_configuration;

	my $client = Gearman::Client->new;
	$client->job_servers(@{$config->{servers}});

	my $task = $self->_gearman_task_from_args($config, $args);
	my $job_id = $client->dispatch_background($task);
    
	return $job_id;
}


# Return configuration, die() on error
sub _configuration($)
{
	my $self = shift;

	my $config = LoadFile(GJS_CONFIG_FILE) or LOGDIE("Unable to read configuration from '" . GJS_CONFIG_FILE . "': $!");
	unless (scalar (@{$config->{servers}})) {
		die "No servers are configured.";
	}

	return $config;	
}


# Validate the job arguments, create Gearman task from parameters or die on error
sub _gearman_task_from_args($$;$)
{
	my $self = shift;
	my $config = shift;
	my $args = shift;

	if (@_ or ($args and ref($args) ne 'HASH' )) {
		die "run() should accept arguments as a hashref";
	}

	my $function_name = $self->_function_name;
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
		uniq => $self->unique,
		on_complete => sub { say STDERR "Complete!" },
		on_fail => sub { say STDERR "Failed for the last time" },
		on_retry => sub { say STDERR "Retry" },
		on_status => sub { say STDERR "Status" },
		retry_count => $self->retries,
		try_timeout => $self->job_timeout,
	});

	return $task;
}


# _run_locally_from_gearman_worker() will temporarily place a Gearman worker to
# this variable so that progress() helper can use it
has '_gearman_worker' => ( is => 'rw' );

# Run locally and right away, blocking the parent process while it gets finished
# (issued either by the Gearman worker)
# Returns result (may be false of undef) on success, die()s on error
sub _run_locally_from_gearman_worker($;$)
{
	my $self = shift;
	my $gearman_worker = shift;

	# Arguments are thawed
	my $args_deserialized = \%{ thaw($gearman_worker->arg) };

	my $result;
	$self->_gearman_worker($gearman_worker);	# will be used by progress()
	eval {
		$result = $self->run_locally($args_deserialized);
	};
	$self->_gearman_worker(undef);				# clear the variable no matter how the job finished
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
	my $self = shift;

	return '' . ref($self);
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

# Initialize (create missing directories) and return a worker log directory path (with trailing slash)
sub _init_and_return_worker_log_dir($)
{
	my ($self) = @_;

	my $config = $self->_configuration;
	my $worker_log_dir = $config->{worker_log_dir} || Sys::Path->logdir . '/gjs/';

    $worker_log_dir =~ s!/*$!/!;    # Add a trailing slash

    unless ( -d $worker_log_dir ) {
    	make_path( $worker_log_dir );
    }

    return $worker_log_dir;
}


1;

no Moose;    # gets rid of scaffolding

1;
