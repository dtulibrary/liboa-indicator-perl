package OA::Indicator::DB::BFI;

use strict;
use warnings;

our $VERSION = '1.0';

sub new
{
    my ($class, $db) = @_;
    my $self = {};

    $self->{'db'} = $db;
    return (bless ($self, $class));
}

sub create
{   
    my ($self) = @_;

    my $db = $self->{'db'};
    $db->sql ('create table if not exists bfi (
                   id              text primary key,
                   publication_id  text,
                   source          text,
                   doc_type        text,                                                                                                                
                   doc_review      text,                                                                                                                
                   doc_level       text,                                                                                                                
                   type            text,                                                                                                                
                   fraction        text,                                                                                                                
                   point           text,                                                                                                                
                   cooperation     text,                                                                                                                
                   title           text,
                   lang            text,
                   mods            text,
                   original_xml    text
              )');
    $db->sql ('create index if not exists bfi_publication_id on bfi (publication_id)');
}

sub load
{
    my ($self, $year) = @_;
    my ($fin);

    if (!open ($fin, "zcat /var/lib/oa-indicator/$year/bfi/bfi.ids.gz |")) {
        die ("fatal: failed to open /var/lib/oa-indicator/$year/bfi/bfi.ids.gz ($!)");
    }
    while (<$fin>) {
        chomp;
        if (m/^#/) {
            next;
        }
        my $rec = {};
        my @fields = split ("\t");
        foreach my $fld (qw(id pubid source doc_type doc_review doc_level type fraction point cooperation title lang)) {
            $rec->{$fld} = shift (@fields);
        }
        $rec->{'source'} = lc ($rec->{'source'});
        $rec->{'mods'} = $rec->{'original_xml'} = '';
        $self->{'db'}->insert ('bfi', $rec);
    }
    close ($fin);
}

sub pubid
{
    my ($self, $id) = @_;

    my $rs = $self->{'db'}->select ('publication_id', 'bfi', "id='$id'");
    my $rec;
    if ($rec = $self->{'db'}->next ($rs)) {
        return ($rec->{'publication_id'});
    } else {
        return (-1);
    }
}

1;

