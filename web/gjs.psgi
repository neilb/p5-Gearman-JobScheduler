use strict;
use warnings;

use GJS;

my $app = GJS->apply_default_middlewares(GJS->psgi_app);
$app;

