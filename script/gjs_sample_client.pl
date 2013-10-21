#!/usr/bin/env perl

use strict;
use warnings;
use Modern::Perl "2012";

use FindBin;
use lib "$FindBin::Bin/../lib";
use lib "$FindBin::Bin/../samples";

use Gearman::JobScheduler;
use Gearman::JobScheduler::Admin;
use NinetyNineBottlesOfBeer;
use Addition;
use AdditionAlwaysFails;
use Data::Dumper;


sub main()
{
	#
	# Server administration example
	#
	my $config = NinetyNineBottlesOfBeer->configuration;

	say STDERR 'server_version(): ' . Dumper(Gearman::JobScheduler::Admin::server_version($config));
	say STDERR 'server_verbose(): ' . Dumper(Gearman::JobScheduler::Admin::server_verbose($config));
	say STDERR 'create_function(): ' . Dumper(Gearman::JobScheduler::Admin::create_function('wooooooo!', $config));
	say STDERR 'drop_function(): ' . Dumper(Gearman::JobScheduler::Admin::drop_function('wooooooo!', $config));

	say STDERR 'show_jobs(): ' . Dumper(Gearman::JobScheduler::Admin::show_jobs($config));
	say STDERR 'show_unique_jobs(): ' . Dumper(Gearman::JobScheduler::Admin::show_unique_jobs($config));
	say STDERR 'cancel_job(): ' . Dumper(Gearman::JobScheduler::Admin::cancel_job('H:tundra.local:17', $config));

	say STDERR 'get_pid(): ' . Dumper(Gearman::JobScheduler::Admin::get_pid($config));
	# say STDERR 'shutdown(): ' . Dumper(Gearman::JobScheduler::Admin::shutdown($config));

	say STDERR 'status(): ' . Dumper(Gearman::JobScheduler::Admin::status($config));
	say STDERR 'workers(): ' . Dumper(Gearman::JobScheduler::Admin::workers($config));


	#
	# Client example
	#
	my $result;
	my $gearman_job_id;
	my $operand_a = 2;
	my $operand_b = 3;

	say STDERR "Will add two numbers (locally)";
	$result = Addition->run_locally({a => $operand_a, b => $operand_b});
	say STDERR "Result of adding $operand_a to $operand_b: $result";

	say STDERR "Will add two numbers on Gearman";
	$result = Addition->run_locally({a => $operand_a, b => $operand_b});
	say STDERR "Result of adding $operand_a to $operand_b: $result";

	say STDERR "Will enqueue adding two numbers on Gearman";
	$gearman_job_id = Addition->enqueue_on_gearman({a => $operand_a, b => $operand_b});
	say STDERR "Gearman job ID: $gearman_job_id";
	eval {
		say STDERR "Status of the addition job: " . Dumper(Gearman::JobScheduler::job_status(Addition->name(), $gearman_job_id));
		sleep(1);
		say STDERR "Status of the addition job after 1 second: " . Dumper(Gearman::JobScheduler::job_status(Addition->name(), $gearman_job_id));
		say STDERR "Log path to the addition job: " . Dumper(Gearman::JobScheduler::log_path_for_gearman_job(Addition->name(), $gearman_job_id));
	};
	if ($@) {
		say STDERR "log_path_for_gearman_job() failed, probably the job isn't running as of now: $@";
	}

	# Failing job
	say STDERR "Will run a failing job locally";
	eval {
		$result = AdditionAlwaysFails->run_on_gearman({a => $operand_a, b => $operand_b});
	};
	if ($@) {
		say STDERR "Addition job failed because: $@";
	} else {
		die "Well, that's unexpected.";
	}

	say STDERR "Will enqueue a failing job on Gearman";
	eval {
		$result = AdditionAlwaysFails->enqueue_on_gearman({a => $operand_a, b => $operand_b});
	};

}

main();
