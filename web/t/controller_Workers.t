use strict;
use warnings;
use Test::More;


use Catalyst::Test 'GJS';
use GJS::Controller::Workers;

ok( request('/workers')->is_success, 'Request should succeed' );
done_testing();
