#!/usr/bin/env perl

use strict;
use warnings;
use Modern::Perl "2012";

use FindBin;
use lib "$FindBin::Bin/sample-jobs";

use YAML qw(LoadFile);
use Gearman::Worker;
use Data::Dumper;

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init({ level => $DEBUG, utf8=>1, layout => "%d{ISO8601} [%P]: %m%n" });

use constant GJS_CONFIG_FILE => 'config.yml';


sub main()
{
	unless (scalar (@ARGV) == 1) {
		LOGDIE("Usage: $0 GearmanJob");
	}

	my $gearman_job_name = $ARGV[0];

    eval {
        ( my $file = $gearman_job_name ) =~ s|::|/|g;
        require $file . '.pm';
        $gearman_job_name->import();
        1;
    } or do
    {
		LOGDIE("Unable to find Gearman job '$gearman_job_name': $@");
    };

	my $config = LoadFile(GJS_CONFIG_FILE) or LOGDIE("Unable to read configuration from '" . GJS_CONFIG_FILE . "': $!");
	unless (scalar (@{$config->{servers}})) {
		LOGDIE("No servers are configured.");
	}

	INFO("Initializing with Gearman job '$gearman_job_name'.");

	my $worker = Gearman::Worker->new;
    $worker->job_servers(@{$config->{servers}});

    my $job = $gearman_job_name->new();

	$worker->register_function($gearman_job_name, sub {
		$job->_run_locally_thaw_args($_[0]->arg);
	});

    INFO("Worker is ready and accepting jobs");
    $worker->work while 1;
}


main();
