#!/usr/bin/env perl

use strict;
use warnings;
use Modern::Perl "2012";

use FindBin;
use lib "$FindBin::Bin/sample-functions";

use GJS;
use NinetyNineBottlesOfBeer;
use Addition;
use AdditionAlwaysFails;
use Data::Dumper;


sub main()
{
	my $result;
	# $result = NinetyNineBottlesOfBeer->run_locally();
	# $result = NinetyNineBottlesOfBeer->run_locally({how_many_bottles => 3});
	# $result = NinetyNineBottlesOfBeer->run_on_gearman();
	# $result = NinetyNineBottlesOfBeer->run_on_gearman({how_many_bottles => 3});
	# $result = NinetyNineBottlesOfBeer->enqueue_on_gearman();

	# $result = NinetyNineBottlesOfBeer->enqueue_on_gearman({how_many_bottles => 3});
	# my $first = NinetyNineBottlesOfBeer->enqueue_on_gearman({how_many_bottles => 20});
	# my $second = NinetyNineBottlesOfBeer->enqueue_on_gearman({how_many_bottles => 20});
	# $result = NinetyNineBottlesOfBeer->enqueue_on_gearman({how_many_bottles => 2000});
	# $result = NinetyNineBottlesOfBeer->run_on_gearman({how_many_bottles => 3, 'this' => 'that', 'hacky hack' => '/etc/passwd'});

	$result = Addition->run_on_gearman({a => 2, b => 3});

	# $result = AdditionAlwaysFails->run_on_gearman({a => 2, b => 3});
	# $result = Addition->run_on_gearman({a => 2, b => 3});
	# $result = AdditionAlwaysFails->enqueue_on_gearman({a => 2, b => 3});

	say STDERR "Job ID: $result";


	# say STDERR "First: $first";
	# say STDERR "Second: $second";

	# # sleep (1);

	# GJS::cancel_gearman_job($second);

	# sleep(1);
	# say STDERR "First: " . Dumper(GJS::get_gearman_status($first));
	# say STDERR "Second: " . Dumper(GJS::get_gearman_status($second));

	# sleep(6);
	# say STDERR "First: " . Dumper(GJS::get_gearman_status($first));
	# say STDERR "Second: " . Dumper(GJS::get_gearman_status($second));

	# # GJS::cancel_gearman_job($result);

	# sleep(3);
	my $status = GJS::get_gearman_status($result);
	say STDERR "Status: " . Dumper($status);

	# sleep(5);
	# $status = GJS::get_gearman_status($result);
	# say STDERR "Status: " . Dumper($status);

	# GJS::_send_email('This is a test', 'My message goes here.');
}

main();
