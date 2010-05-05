#!/usr/bin/perl

package KiokuDB::Backend::CouchDB;
use Moose;

use Moose::Util::TypeConstraints;

use Data::Stream::Bulk::Util qw(bulk);

use AnyEvent::CouchDB;
use JSON;
use Carp 'confess';

use namespace::clean -except => 'meta';

our $VERSION = "0.05";

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

has conflicts => (
    is      => 'rw',
    isa     => enum([qw{ overwrite confess ignore }]),
    default => 'confess'
);
    

sub BUILD {
    my $self = shift;

    if ( $self->create ) {
        my $e = do {local $@; eval { $self->db->create->recv }; $@ };

        # Throw errors except if its because the database already exists
        if ( $e ) {
            if ( my($error) = grep { exists $_->{error} } @$e ) {
                if( $error->{error} ne 'file_exists' ) {
                    die "$error->{error}: $error->{reason}";
                }
            }
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
                $collapsed->{_rev} = $doc->{_rev} if $doc->{_rev};
                last find_rev;
            }
        }
    }

    my $data = $self->db->bulk_docs(\@docs)->recv;

    if ( my @errors = grep { exists $_->{error} } @$data ) {

        if($self->conflicts eq 'confess') {
            confess "Errors in update: " . join(", ", map { "$_->{error} (on ID $_->{id})" } @errors);
        } elsif($self->conflicts eq 'overwrite') {
            my @conflicts;
            my @other_errors;
            for(@errors) {
                if($_->{error} eq 'conflict') {
                    push @conflicts, $_->{id};
                } else {
                    push @other_errors, $_;
                }
            }
            if(@other_errors) {
                confess "Errors in update: " . join(", ", map { "$_->{error} (on ID $_->{id})" } @other_errors);
            }
            
            # Updating resulted in conflicts that we handle by overwriting the change
            my $old_docs = $db->open_docs([@conflicts])->recv;
            if(exists $old_docs->{error}) {
                confess "Updating ids ", join(', ', @conflicts), " failed during conflict resolution: $old_docs->{error}.";
            }
            my @old_docs = @{$old_docs->{rows}};
            my @re_update_docs;
            foreach my $old_doc (@old_docs) {
                my($new_doc) = grep {$old_doc->{doc}{_id} eq $_->{_id}} @docs;
                $new_doc->{_rev} = $old_doc->{doc}{_rev};
            }
            # Handle errors that has arised when trying the second update
            if(@errors = grep { exists $_->{error} } @{$self->db->bulk_docs(\@re_update_docs)->recv}) {
                confess "Updating ids ", join(', ', @conflicts), " failed during conflict resolution: ",
                    join(', ', map { $_->{error} . ' on ' . $_->{id} } @errors);
            }
        }
        # $self->conflicts eq 'ignore' here, so don't do anything
    }

    foreach my $rev ( map { $_->{rev} } @$data ) {
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

sub get_from_storage {
    my ( $self, @ids ) = @_;

    my $db = $self->db;

    my $cv = $db->open_docs(\@ids);

    my $data = $cv->recv;
    
    map { $self->deserialize($_) }
        map {$_->{doc}}
        grep {exists $_->{doc}}
        @{ $data->{rows} };
}

sub deserialize {
    my ( $self, $doc ) = @_;

    confess "no doc provided" unless $doc;

    my %doc = %{ $doc };

    return $self->expand_jspon(\%doc, backend_data => $doc );
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

1;

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

L<http://github.com/mzedeler/kiokudb-backend-couchdb>

=head1 AUTHOR

Yuval Kogman E<lt>nothingmuch@woobling.orgE<gt>

=head1 CONTRIBUTORS

Michael Zedeler E<lt>michael@zedeler.dk<gt>, Anders Bruun Borch E<lt>cyborch@deck.dk<gt>

=head1 COPYRIGHT

    Copyright (c) 2008, 2009 Yuval Kogman, Infinity Interactive. All
    rights reserved This program is free software; you can redistribute
    it and/or modify it under the same terms as Perl itself.

    Copyright (c) 2010 Leasingbørsen. All rights reserved. This program
    is free software; you can redistribute it and/or modify it under 
    the same terms as Perl itself.

=cut
