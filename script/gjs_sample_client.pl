#!/usr/bin/env perl

use strict;
use warnings;
use Modern::Perl "2012";

use FindBin;
use lib "$FindBin::Bin/../lib";
use lib "$FindBin::Bin/../samples";

use Gearman::JobScheduler;
use NinetyNineBottlesOfBeer;
use Addition;
use AdditionAlwaysFails;
use Data::Dumper;


sub main()
{
	my $result;
	my $gearman_job_id;

	# Simple function which adds two numbers and returns the results
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
	say STDERR "Status of the addition job: " . Dumper(Gearman::JobScheduler::job_status(Addition->name(), $gearman_job_id));
	sleep(1);
	say STDERR "Status of the addition job after 1 second: " . Dumper(Gearman::JobScheduler::job_status(Addition->name(), $gearman_job_id));
	say STDERR "Log path to the addition job: " . Dumper(Gearman::JobScheduler::log_path_for_gearman_job(Addition->name(), $gearman_job_id));

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
