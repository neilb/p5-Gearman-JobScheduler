#!/usr/bin/env perl

use strict;
use warnings;
use Modern::Perl "2012";

use FindBin;
use lib "$FindBin::Bin/../lib";
use lib "$FindBin::Bin/../samples";

use Gearman::JobScheduler;
use Gearman::JobScheduler::Configuration;
use Gearman::JobScheduler::Worker;

use constant PM_MAX_PROCESSES => 32;

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init({ level => $DEBUG, utf8=>1, layout => "%d{ISO8601} [%P]: %m%n" });

use Getopt::Long qw(:config auto_help);
use Pod::Usage;


sub main()
{
	# Initialize with default configuration (to be customized later)
	my $config = Gearman::JobScheduler::_default_configuration();


	# Override default configuration options from the command line if needed
	GetOptions(
		'server:s@' => \$config->gearman_servers,
		'worker_log_dir:s' => \$config->worker_log_dir,
		'notif_email:s@' => \$config->notifications_emails,
		'notif_from:s' => \$config->notifications_from_address,
		'notif_subj_prefix:s' => \$config->notifications_subject_prefix,
	);

	# Function name, path to function module or path to directory with all functions
	unless (scalar (@ARGV) == 1) {
		pod2usage(1);
	}
	my $gearman_function_name_or_directory = $ARGV[0];

	INFO("Will use Gearman servers: " . join(' ', @{$config->gearman_servers}));
	if (scalar @{$config->notifications_emails}) {
		INFO('Will send notifications about failed jobs to: ' . join(' ', @{$config->notifications_emails}));
		INFO('(emails will be sent from "' . $config->notifications_from_address
			     . '" and prefixed with "' . $config->notifications_subject_prefix . '")');
	} else {
		INFO('Will not send notifications anywhere about failed jobs.');
	}

	if (-d $gearman_function_name_or_directory) {

		# Run all workers
		Gearman::JobScheduler::Worker::run_all_workers($config, $gearman_function_name_or_directory);

	} else {

		# Run single worker
		Gearman::JobScheduler::Worker::run_worker($config, $gearman_function_name_or_directory);
	}

}


main();


=head1 NAME

worker.pl - Start one or all GJS workers

=head1 SYNOPSIS

worker.pl [options] GearmanFunction

or:

worker.pl [options] path/to/GearmanFunction.pm

or:

worker.pl [options] path_to/dir_with/gearman_functions/


 Options:
	--server=host[:port]            use Gearman server at host[:port] (multiple allowed)
	--worker_log_dir=/path/to/logs  directory where worker logs should be stored
	--notif_email=jdoe@example.com  whom to send notification emails about failed jobs to (multiple allowed)
	--notif_from=gjs@example.com    sender of the notification emails about failed jobs
	--notif_subj_prefix="[GJS]"     prefix of the subject line of notification emails about failed jobs

=cut
