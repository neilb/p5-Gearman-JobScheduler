#!/usr/bin/env perl

use strict;
use warnings;
use Modern::Perl "2012";

use FindBin;
use lib "$FindBin::Bin/sample-functions";

use YAML qw(LoadFile);
use Gearman::Worker;
use Data::Dumper;

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init({ level => $DEBUG, utf8=>1, layout => "%d{ISO8601} [%P]: %m%n" });

use constant GJS_CONFIG_FILE => 'config.yml';


sub main()
{
	unless (scalar (@ARGV) == 1) {
		die "Usage: $0 GearmanFunction\n   or: $0 path/to/GearmanFunction.pm\n";
	}

	my $gearman_function_name = $ARGV[0];

    eval {
    	if ($gearman_function_name =~ /\.pm$/) {
    		# /somewhere/Foo/Bar.pm

    		# Expect the package to return its name so that we'll know how to call it:
    		# http://stackoverflow.com/a/9850017/200603
    		$gearman_function_name = require $gearman_function_name;
    		if ($gearman_function_name . '' eq '1') {
    			die "The function package should return __PACKAGE__ at the end of the file instead of just 1.";
    		}
	        $gearman_function_name->import();
    		1;
    	} else {
    		# Foo::Bar
	        ( my $file = $gearman_function_name ) =~ s|::|/|g;
	        require $file . '.pm';
	        $gearman_function_name->import();
	        1;
    	}
    } or do
    {
		LOGDIE("Unable to find Gearman function '$gearman_function_name': $@");
    };

	my $config = LoadFile(GJS_CONFIG_FILE) or LOGDIE("Unable to read configuration from '" . GJS_CONFIG_FILE . "': $!");
	unless (scalar (@{$config->{servers}})) {
		LOGDIE("No servers are configured.");
	}

	INFO("Initializing with Gearman function '$gearman_function_name'.");

	my $worker = Gearman::Worker->new;
    $worker->job_servers(@{$config->{servers}});
    $worker->debug(1);

	$worker->register_function($gearman_function_name => sub {
		my ($gearman_job) = shift;

		my $job_handle = $gearman_job->{handle};
		my $result;
		eval {
			$result = $gearman_function_name->_run_locally_from_gearman_worker($gearman_job);
		};
		if ($@) {
			LOGDIE("Gearman job with handle '$job_handle' died: $@");
		}

		return $result;
	});

    INFO("Worker is ready and accepting jobs");
    $worker->work while 1;
}


main();
