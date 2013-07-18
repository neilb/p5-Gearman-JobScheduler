use strict;
use warnings;
use Test::More;


use Catalyst::Test 'GJS';
use GJS::Controller::Queue;

ok( request('/queue')->is_success, 'Request should succeed' );
done_testing();
