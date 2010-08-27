package KiokuX::Role::CouchDB::View;

use Moose::Role;

use namespace::clean -except => 'meta';

# view() is a non-compliant method that can be called directly to query 
# couchdb views and have all KiokuDB objects instantiated when needed

sub view {
    my($self, $name, $options) = @_;
    return $self->deserialize($self->db->view($name, $options)->recv);
}

1;
