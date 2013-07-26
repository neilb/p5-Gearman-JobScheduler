#!/usr/bin/env perl

use strict;
use warnings;
use Modern::Perl "2012";

use FindBin;
use lib "$FindBin::Bin/sample-functions";

use NinetyNineBottlesOfBeer;
use Addition;
use AdditionAlwaysFails;


sub main()
{
	my $result;
	# $result = NinetyNineBottlesOfBeer->run_locally();
	# $result = NinetyNineBottlesOfBeer->run_locally({how_many_bottles => 3});
	# $result = NinetyNineBottlesOfBeer->run_on_gearman();
	# $result = NinetyNineBottlesOfBeer->run_on_gearman({how_many_bottles => 3});
	# $result = NinetyNineBottlesOfBeer->enqueue_on_gearman();
	# $result = NinetyNineBottlesOfBeer->enqueue_on_gearman({how_many_bottles => 3});
	# $result = NinetyNineBottlesOfBeer->run_on_gearman({how_many_bottles => 3, 'this' => 'that', 'hacky hack' => '/etc/passwd'});

	# $result = Addition->run_on_gearman({a => 2, b => 3});

	$result = AdditionAlwaysFails->run_on_gearman({a => 2, b => 3});

	say STDERR "Result: $result";
}

main();
