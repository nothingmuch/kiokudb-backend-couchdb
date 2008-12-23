#!/usr/bin/perl

package KiokuDB::Backend::CouchDB;
use Moose;

use Data::Stream::Bulk::Util qw(bulk);

use AnyEvent::CouchDB;
use JSON;

use namespace::clean -except => 'meta';

our $VERSION = "0.01";

with qw(
    KiokuDB::Backend
    KiokuDB::Backend::Serialize::JSPON
    KiokuDB::Backend::Role::UnicodeSafe
    KiokuDB::Backend::Role::Clear
    KiokuDB::Backend::Role::Scan
    KiokuDB::Backend::Role::Query::Simple::Linear
);

has db => (
    isa => "AnyEvent::CouchDB::Database",
    is  => "ro",
    handles => [qw(document)],
);

has '+id_field'    => ( default => "_id" );
has '+class_field' => ( default => "class" );
has '+deleted_field' => ( default => "_deleted" );

sub new_from_dsn_params {
    my ( $self, %args ) = @_;

    my $db = exists $args{db}
        ? couch($args{uri})->db($args{db})
        : couchdb($args{uri});

    $self->new(%args, db => $db);
}

sub delete {
    my ( $self, @ids_or_entries ) = @_;

    my $db = $self->db;

    my @cvs = map { $db->open_doc($_) } grep { not ref } @ids_or_entries;

    my @docs = (
        ( map { $_->backend_data } grep { ref } @ids_or_entries ),
        ( map { $_->recv } @cvs ),
    );

    my $cv = $db->bulk_docs(
        [ map {
            my $doc = blessed($_) ? $_->recv : $_;

            {
                _id      => $doc->{_id},
                _rev     => $doc->{_rev},
                _deleted => JSON::true,
            }
        } @docs ],
    );

    $cv->recv;
}

sub insert {
    my ( $self, @entries ) = @_;

    my @docs;

    my $db = $self->db;

    foreach my $entry ( @entries ) {
        my $collapsed = $self->collapse_jspon($entry);

        push @docs, $collapsed;

        $entry->backend_data($collapsed);

        if ( my $prev = $entry->prev ) {
            my $doc = $prev->backend_data;
            $collapsed->{_rev} = $doc->{_rev};
        }
    }

    my $cv = $self->db->bulk_docs(\@docs);

    my $data = $cv->recv;

    foreach my $rev ( map { $_->{rev} } @{ $data->{new_revs} } ) {
        ( shift @docs )->{_rev} = $rev;
    }
}

sub get {
    my ( $self, @uids ) = @_;

    my @ret;

    my $db = $self->db;

    return (
        map { $self->deserialize($_->recv) }
        map { $db->open_doc($_) } @uids
    );
}

sub deserialize {
    my ( $self, $doc ) = @_;

    my %doc = %{ $doc };

    return $self->expand_jspon(\%doc, backend_data => $doc );
}

sub exists {
    my ( $self, @uids ) = @_;
    my $db = $self->db;
    map { local $@; scalar eval { $_->recv; 1 } } map { $db->open_doc($_) } @uids;
}

sub clear {
    my $self = shift;

    $self->db->drop->recv;
    $self->db->create->recv;
}

sub all_entries {
    my $self = shift;

    my $db = $self->db;

    my $rows = $db->all_docs->recv->{rows};

    # FIXME iterative
    bulk( $self->get(map { $_->{id} } @$rows) );
}

__PACKAGE__->meta->make_immutable;

__PACKAGE__

__END__

