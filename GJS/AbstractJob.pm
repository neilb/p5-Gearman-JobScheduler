package GJS::AbstractJob;

use strict;
use warnings;
use Modern::Perl "2012";

use Moose::Role;

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

use constant GJS_CONFIG_FILE => 'config.yml';

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
# * returns result on success (serializable by the Storable module)
#     * the result will be discarded if the job is ordered on Gearman as a background process
# * die()s on error
# * writes log to STDOUT or STDERR (preferably the latter)
requires 'run';


# Return the timeout of each job
# ------------------------------
#
# Returns the timeout (in seconds) of each job or 0 if there's no timeout.
requires 'job_timeout';


# Return the number of retries for each job
# -----------------------------------------
#
# Returns a number of retries each job will be attempted at.
# Return 0 if the job should not be retried.
requires 'retries';


# Return true if the job is "unique"
# ----------------------------------
#
# Returns true if two or more tasks with the same parameters can not be run at
# the same and instead should be merged into one.
requires 'unique';


#
# =========================
# END OF ABSTRACT INTERFACE
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

	my $ug    = new Data::UUID;
	my $uuid = $ug->create_str();

	my $job_name = '' . ref($self);
	my $job_args = $args ? '{' . join(', ', map { "$_ => $args->{$_}" } keys $args) . '}' : '';
	my $job_id = "$job_name($job_args).$uuid";

	my $starting_job_message = "Starting job ID \"$job_id\" ...";
	my $finished_job_message;

	_reset_log4perl();
	INFO($starting_job_message);

	Log::Log4perl->easy_init({
		level => $DEBUG,
		utf8=>1,
		file => 'out.txt',	# do not use STDERR / STDOUT here because it would end up with recursion
		layout => "%d{ISO8601} [%P]: %m"
	});


	# Tie STDOUT / STDERR to Log4perl handler
	tie *STDOUT, "_ErrorLogTrapper";
	tie *STDERR, "_ErrorLogTrapper";

	my $result;

	eval {

		# say STDERR "Job name: " . $job_name;
		# say STDERR "Job args: " . $job_args;
		# say STDERR "Job ID: " . $job_id;

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

	my $task = $self->_task_from_args($config, $args);
	my $result_ref = $client->do_task($task);
    # say STDERR "Serialized result: " . Dumper($result_ref);

	my $result_deserialized = thaw($$result_ref);
	$result_deserialized = $$result_deserialized;

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

	my $task = $self->_task_from_args($config, $args);
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
sub _task_from_args($$;$)
{
	my $self = shift;
	my $config = shift;
	my $args = shift;

	if (@_ or ($args and ref($args) ne 'HASH' )) {
		die "run() should accept arguments as a hashref";
	}

	my $job_name = '' . ref($self);
	unless ($job_name) {
		die "Unable to determine job name.";
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

	my $task = Gearman::Task->new($job_name, \$args_serialized, {
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


# Run locally and right away, blocking the parent process while it gets finished
# (issued either by the original caller or the Gearman worker)
# Returns result (may be false of undef) on success, die()s on error
sub _run_locally_thaw_args($;$)
{
	my $self = shift;
	my $args = shift;

	my $args_deserialized = \%{ thaw($args) };

	my $result;
	eval {
		$result = $self->run_locally($args_deserialized);
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


{
	# Log4perl's trapper module
	# (http://log4perl.sourceforge.net/releases/Log-Log4perl/docs/html/Log/Log4perl/FAQ.html#e95ee)
	package _ErrorLogTrapper;

	use strict;
	use warnings;

	use Log::Log4perl qw(:easy);

	sub TIEHANDLE {
		my $class = shift;
		bless [], $class;
	}

	sub PRINT {
		my $self = shift;
		$Log::Log4perl::caller_depth++;
		DEBUG @_;
		$Log::Log4perl::caller_depth--;
	}

	1;
}


# Reset Log::Log4perl to write to the STDERR / STDOUT and not to file
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
