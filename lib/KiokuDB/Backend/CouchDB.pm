#!/usr/bin/perl

package KiokuDB::Backend::CouchDB;
use Moose;

use Data::Stream::Bulk::Util qw(bulk);

use AnyEvent::CouchDB;
use JSON;

use namespace::clean -except => 'meta';

our $VERSION = "0.02";

with qw(
    KiokuDB::Backend
    KiokuDB::Backend::Serialize::JSPON
    KiokuDB::Backend::Role::UnicodeSafe
    KiokuDB::Backend::Role::Clear
    KiokuDB::Backend::Role::Scan
    KiokuDB::Backend::Role::Query::Simple::Linear
);

has create => (
    isa => "Bool",
    is  => "ro",
    default => 0,
);

sub BUILD {
    my $self = shift;

    if ( $self->create ) {
        my $e = do {local $@; eval { $self->db->create->recv }; $@ };

        if ( $e ) {
            die $e unless $e =~ /database_already_exists/;
        }
    }
}

has db => (
    isa => "AnyEvent::CouchDB::Database",
    is  => "ro",
    handles => [qw(document)],
);

has '+id_field'    => ( default => "_id" );
has '+class_field' => ( default => "class" );
has '+class_meta_field' => ( default => "class_meta" );
has '+deleted_field' => ( default => "_deleted" );

#has _prefetch => (
#    isa => "HashRef",
#    is  => "ro",
#    default => sub { +{} },
#);

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

# this is actually slower for some reason
#sub prefetch {
#    my ( $self, @uids ) = @_;
#
#    my $db = $self->db;
#    my $p = $self->_prefetch;
#
#    foreach my $uid ( @uids ) {
#        $p->{$uid} ||= $db->open_doc($uid);
#    }
#}

sub get {
    my ( $self, @uids ) = @_;

    my @ret;

    my $db = $self->db;
    #my $p = $self->_prefetch;

    return (
        map { $self->deserialize($_->recv) }
        map { #delete($p->{$_}) ||
            $db->open_doc($_) } @uids
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

=pod

=head1 NAME

KiokuDB::Backend::CouchDB - CouchDB backend for L<KiokuDB>

=head1 SYNOPSIS

    KiokuDB->connect( "couchdb:uri=http://127.0.0.1:5984/database" );

=head1 DESCRIPTION

This backend provides L<KiokuDB> support for CouchDB using L<AnyEvent::CouchDB>.

Note that this is the slowest backend of all for reading data, due to the
latency in communicating with CouchDB over HTTP.

=head1 ATTRIBUTES

=over 4

=item db

An L<AnyEvent::CouchDB::Database> instance.

Required.

=item create

Whether or not to try and create the database on instantiaton.

Defaults to false.

=back

=head1 VERSION CONTROL

L<http://github.com/nothingmuch/kiokudb-backend-couchdb>

=head1 AUTHOR

Yuval Kogman E<lt>nothingmuch@woobling.orgE<gt>

=head1 COPYRIGHT

	Copyright (c) 2008 Yuval Kogman, Infinity Interactive. All rights
    reserved This program is free software; you can redistribute
	it and/or modify it under the same terms as Perl itself.

=cut
