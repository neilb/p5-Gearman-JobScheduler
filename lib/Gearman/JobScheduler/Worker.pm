package Gearman::JobScheduler::Worker;

#
# GJS worker helpers
#

use strict;
use warnings;
use Modern::Perl "2012";

use Gearman::JobScheduler;
use Gearman::JobScheduler::Configuration;

use Gearman::XS qw(:constants);
use Gearman::XS::Worker;

use Parallel::ForkManager;

use Data::Dumper;

use constant PM_MAX_PROCESSES => 32;

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init({ level => $DEBUG, utf8=>1, layout => "%d{ISO8601} [%P]: %m%n" });



# Import Gearman function Perl module by path or name
sub import_gearman_function($)
{
	my ($path_or_name) = shift;

    eval {
    	if ($path_or_name =~ /\.pm$/) {
    		# /somewhere/Foo/Bar.pm

    		# Expect the package to return its name so that we'll know how to call it:
    		# http://stackoverflow.com/a/9850017/200603
    		$path_or_name = require $path_or_name;
    		if ($path_or_name . '' eq '1') {
    			LOGDIE("The function package should return __PACKAGE__ at the end of the file instead of just 1.");
    		}
	        $path_or_name->import();
    		1;
    	} else {
    		# Foo::Bar
	        ( my $file = $path_or_name ) =~ s|::|/|g;
	        require $file . '.pm';
	        $path_or_name->import();
	        1;
    	}
    } or do
    {
		LOGDIE("Unable to find Gearman function in '$path_or_name': $@");
    };

    return $path_or_name;
}


# Run a single worker
sub run_worker($$)
{
	my ($config, $gearman_function_name_or_path) = @_;

	my $gearman_function_name = import_gearman_function($gearman_function_name_or_path);
	INFO("Initializing with Gearman function '$gearman_function_name' from '$gearman_function_name_or_path'.");

	my $ret;
	my $worker = new Gearman::XS::Worker;

	$ret = $worker->add_servers(join(',', @{$config->gearman_servers}));
	unless ($ret == GEARMAN_SUCCESS) {
		LOGDIE("Unable to add Gearman servers: "  . $worker->error());
	}

	INFO("Job priority: " . $gearman_function_name->priority());

	$ret = $worker->add_function(
		$gearman_function_name,
		$gearman_function_name->timeout() * 1000,	# in milliseconds
		sub {
			my ($gearman_job) = shift;

			my $job_handle = $gearman_job->handle();
			my $result;
			eval {
				$result = $gearman_function_name->_run_locally_from_gearman_worker($config, $gearman_job);
			};
			if ($@) {
				INFO("Gearman job '$job_handle' died: $@");
				$gearman_job->send_fail();
				return undef;
			} else {
				$gearman_job->send_complete($result);
				return $result;
			}
		},
		0
	);
	unless ($ret == GEARMAN_SUCCESS) {
		LOGDIE("Unable to add Gearman function '$gearman_function_name': " . $worker->error());
	}

    INFO("Worker is ready and accepting jobs");
    while (1) {
		$ret = $worker->work();
		unless ($ret == GEARMAN_SUCCESS) {
			LOGDIE("Unable to execute Gearman job: " . $worker->error());
		}
	}
}


# Run all workers
sub run_all_workers($$)
{
	my ($config, $gearman_functions_directory) = @_;

	# Run all workers
	INFO("Initializing with all functions from directory '$gearman_functions_directory'.");
	my @function_modules = glob $gearman_functions_directory . '/*.pm';
	if (scalar @function_modules > PM_MAX_PROCESSES) {
		LOGDIE("Too many workers to be started.");
	}

	my $pm = Parallel::ForkManager->new(PM_MAX_PROCESSES);

	foreach my $function_module (@function_modules) {

		$pm->start($function_module) and next;	# do the fork

		run_worker($config, $function_module);

		$pm->finish; # do the exit in the child process

	}

	INFO("All workers ready.");
	$pm->wait_all_children;
}


1;
