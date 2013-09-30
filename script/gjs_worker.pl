#!/usr/bin/env perl

=head1 NAME

worker.pl - Start one or all GJS workers

=head1 SYNOPSIS

	# Run 1 instance of the "NinetyNineBottlesOfBeer" Gearman function
	worker.pl NinetyNineBottlesOfBeer

or:

	# Run 1 instance of the Gearman function from "path/to/NinetyNineBottlesOfBeer.pm"
	worker.pl path/to/NinetyNineBottlesOfBeer.pm

or:

	# Run 1 instance of each Gearman function from "path_to/dir_with/gearman_functions/"
	worker.pl path_to/dir_with/gearman_functions/

or:

	# Run 4 instances of the "NinetyNineBottlesOfBeer" Gearman function
	worker.pl NinetyNineBottlesOfBeer 4

or:

	# Run 8 instances of the Gearman function from "path/to/NinetyNineBottlesOfBeer.pm"
	worker.pl path/to/NinetyNineBottlesOfBeer.pm 8

or:

	# Run 2 instances of each Gearman function from "path_to/dir_with/gearman_functions/"
	worker.pl path_to/dir_with/gearman_functions/ 2

=cut

use strict;
use warnings;
use Modern::Perl "2012";

use FindBin;
use lib "$FindBin::Bin/../lib";
use lib "$FindBin::Bin/../samples";

use Gearman::JobScheduler;
use Gearman::JobScheduler::Configuration;
use Gearman::JobScheduler::Worker;

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init({ level => $DEBUG, utf8=>1, layout => "%d{ISO8601} [%P]: %m%n" });

use Pod::Usage;


sub main()
{
	# Function name, path to function module or path to directory with all functions
	unless (scalar (@ARGV) == 1 or scalar (@ARGV) == 2) {
		pod2usage(1);
	}
	my $gearman_function_name_or_directory = $ARGV[0];
	my $number_of_instances = $ARGV[1] || 1;

	if (-d $gearman_function_name_or_directory) {

		# Run all workers
		Gearman::JobScheduler::Worker::run_all_workers($gearman_function_name_or_directory, $number_of_instances);

	} else {

		# Run single worker
		Gearman::JobScheduler::Worker::run_worker($gearman_function_name_or_directory, $number_of_instances);
	}

}


main();
