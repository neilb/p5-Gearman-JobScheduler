#!/usr/bin/env perl

use strict;
use warnings;
use Modern::Perl "2012";

use FindBin;
use lib "$FindBin::Bin/sample-functions";

use NinetyNineBottlesOfBeer;


sub main()
{
	my $test = NinetyNineBottlesOfBeer->new();
	my $result;
	# $result = $test->run_locally({how_many_bottles => 3});
	# $result = $test->run_locally({how_many_bottles => 3});
	$result = $test->run_on_gearman({how_many_bottles => 3});
	# $result = $test->run_on_gearman();
	# $result = $test->enqueue_on_gearman();
	# $result = $test->run_on_gearman({how_many_bottles => 3, 'this' => 'that', 'hacky hack' => '/etc/passwd'});

	say STDERR "Result: $result";
}

main();
