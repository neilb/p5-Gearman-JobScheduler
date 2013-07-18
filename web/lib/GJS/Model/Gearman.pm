package GJS::Model::Gearman;
use Moose;
use namespace::autoclean;

extends 'Catalyst::Model';

=head1 NAME

GJS::Model::Gearman - Catalyst Model

=head1 DESCRIPTION

Catalyst Model.


=encoding utf8

=head1 AUTHOR

Linas Valiukas

=head1 LICENSE

This library is free software. You can redistribute it and/or modify
it under the same terms as Perl itself.

=cut

__PACKAGE__->meta->make_immutable;

1;
