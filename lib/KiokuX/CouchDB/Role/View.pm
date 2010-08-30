package KiokuX::CouchDB::Role::View;

use Moose::Role;
use Data::Dmap;
use Carp 'croak';
use Scalar::Util 'blessed';

use namespace::clean -except => 'meta';

# view() is a non-compliant method that can be called directly to query 
# couchdb views and have all KiokuDB objects instantiated when needed
# $name is a name of a CouchDB view that can contain complete KiokuDB
# entries or references (in serialized form, of course).
sub view {
    my($self, $name, $options) = @_;

    my($result) = dmap {
        if(ref eq 'HASH') {
            if($_->{key} and $_->{value} and blessed $_->{value}) {
                $_ = $self->linker->expand_object($_->{value});
            }
        } elsif(blessed $_ and $_->isa('KiokuDB::Reference')) {
            my $ref_obj = $_;
            $_ = \{'Unlinked reference' => $_};
            $self->linker->queue_ref($ref_obj, $_);
        }
        $_
    } $self->backend->deserialize($self->backend->db->view($name, $options)->recv);

    $self->linker->load_queue;

    return $result;
}

1;
