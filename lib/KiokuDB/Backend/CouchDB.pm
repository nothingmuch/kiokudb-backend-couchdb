#!/usr/bin/perl

package KiokuDB::Backend::CouchDB;
use Moose;

use Data::Stream::Bulk::Util qw(bulk);

use AnyEvent::CouchDB;
use JSON;

use namespace::clean -except => 'meta';

our $VERSION = "0.04";

with qw(
    KiokuDB::Backend
    KiokuDB::Backend::Serialize::JSPON
    KiokuDB::Backend::Role::UnicodeSafe
    KiokuDB::Backend::Role::Clear
    KiokuDB::Backend::Role::Scan
    KiokuDB::Backend::Role::Query::Simple::Linear
    KiokuDB::Backend::Role::TXN::Memory
    KiokuDB::Backend::Role::Concurrency::POSIX
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

sub commit_entries {
    my ( $self, @entries ) = @_;

    my @docs;

    my $db = $self->db;

    foreach my $entry ( @entries ) {
        my $collapsed = $self->collapse_jspon($entry); 

        push @docs, $collapsed;

        $entry->backend_data($collapsed);

        my $prev = $entry;
        find_rev: while ( $prev = $prev->prev ) {
            if ( my $doc = $prev->backend_data ) {
                $collapsed->{_rev} = $doc->{_rev};
                last find_rev;
            }
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
    my ( $self, @ids ) = @_;

    my $db = $self->db;

    $self->txn_loaded_entries(map { $self->deserialize($_->recv) } map { $db->open_doc($_) } @ids);
}

sub deserialize {
    my ( $self, $doc ) = @_;

    my %doc = %{ $doc };

    return $self->expand_jspon(\%doc, backend_data => $doc );
}

sub exists {
    my ( $self, @ids ) = @_;

    my $db = $self->db;
    map { local $@; scalar eval { $self->txn_loaded_entries($self->deserialize($_->recv)) } } map { $db->open_doc($_) } @ids;
}

sub clear {
    my $self = shift;

    # FIXME TXN

    $self->db->drop->recv;
    $self->db->create->recv;
}

sub all_entries {
    my ( $self, %args ) = @_;

    # FIXME pagination
    my @ids = map { $_->{id} } @{ $self->db->all_docs->recv->{rows} };

    if ( my $l = $args{live_objects} ) {
        my %entries;
        @entries{@ids} = $l->ids_to_entries(@ids);

        my @missing = grep { not $entries{$_} } @ids;

        @entries{@missing} = $self->get(@missing);

        return bulk(values %entries);
    } else {
        return bulk($self->get(@ids));
    }
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

=head1 TRANSACTION SUPPORT

Since CouchDB supports atomicity by using optimistic concurrency locking
transactions are be implemented by deferring all operations until the final
commit.

This means transactions are memory bound so if you are inserting or modifying
lots of data it might be wise to break it down to smaller transactions.

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

    Copyright (c) 2008, 2009 Yuval Kogman, Infinity Interactive. All
    rights reserved This program is free software; you can redistribute
    it and/or modify it under the same terms as Perl itself.

=cut
