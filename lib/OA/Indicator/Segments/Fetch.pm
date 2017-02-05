package OA::Indicator::Segments::Fetch;

use strict;
use warnings;
use OA::Indicator::DB::MXD;
use OA::Indicator::DB::Fulltext;

sub new
{
    my ($class, $db, $oai) = @_;
    my $self = {};

    $self->{'db'} = $db;
    $self->{'oai'} = $oai;
    return (bless ($self, $class));
}

sub process
{
    my ($self, $type) = @_;

    my $count = {total => 0, requests => 0, noft => 0, notdo => 0, notoa => 0};
    my $records = {};
    my $rs = $self->{'db'}->select ('id', 'records', 'scoped=1 and screened=1');
    my $rec;
    while ($rec = $self->{'db'}->next ($rs)) {
        $count->{'total'}++;
        $records->{$rec->{'id'}} = {
            id                  => $rec->{'id'},
            fulltext_link       => 0,
            fulltext_link_oa    => 0,
            fulltext_downloaded => 0,
            fulltext_verified   => 0,
            fulltext_pdf        => 0,
        };
    }
    $self->{'oai'}->log ('i', "starting fulltext checks of $count->{'total'} records");
    my $mxd = new OA::Indicator::DB::MXD ($self->{'db'}, $self->{'oai'});
    my $ft = new OA::Indicator::DB::Fulltext ($self->{'oai'});
    $ft->create ();
    foreach my $id (keys (%{$records})) {
        $count->{'ft'} = 0;
        foreach my $rc ($mxd->fulltext ($id)) {
            if ($rc->{'type'} !~ m/^(loc|rem)$/) {
                $count->{'invalid-type'}{$rc->{'type'}}++;
                next;
            }
            if ($rc->{'url'} !~ m/^https?:/i) {
                my ($protocol) = split (':', $rc->{'url'});
                if (!$protocol) {
                    $protocol = '-undefined-';
                }
                $count->{'invalid-protocol'}{$protocol}++;
                next;
            }
            $records->{$id}{'fulltext_link'} = 1;
            if ($rc->{'access'} ne 'oa') {
                $count->{'invalid-access'}{$rc->{'access'}}++;
                next;
            }
            $records->{$id}{'fulltext_link_oa'} = 1;
            $count->{'ft'}++;
            $count->{'requests'}++;
            $ft->request ($id, $rc->{'type'}, $rc->{'url'});
        }
        $self->update ($records->{$id});
        if (!$count->{'ft'}) {
            $count->{'no-fulltext'}++;
        }
    }
    $self->{'oai'}->log ('i', "placed $count->{'requests'} requests out of $count->{'total'} records");
    $self->{'oai'}->log ('i', "$count->{'no-fulltext'} have no fulltext");
    my $n = 0;
    my @s = ();
    foreach my $key (sort (keys (%{$count->{'invalid-type'}}))) {
        $n += $count->{'invalid-type'}{$key};
        push (@s, $key . ': ' . $count->{'invalid-type'}{$key});
    }
    if ($n) {
        $self->{'oai'}->log ('i', "$n are of invalid type - " .  join (', ', @s));
    }
    $n = 0;
    @s = ();
    foreach my $key (sort (keys (%{$count->{'invalid-protocol'}}))) {
        $n += $count->{'invalid-protocol'}{$key};
        push (@s, $key . ': ' . $count->{'invalid-protocol'}{$key});
    }
    if ($n) {
        $self->{'oai'}->log ('i', "$n have an invalid protocol - " .  join (', ', @s));
    }
    $n = 0;
    @s = ();
    foreach my $key (sort (keys (%{$count->{'invalid-access'}}))) {
        $n += $count->{'invalid-access'}{$key};
        push (@s, $key . ': ' . $count->{'invalid-access'}{$key});
    }
    if ($n) {
        $self->{'oai'}->log ('i', "$n are not open access - " .  join (', ', @s));
    }
    $ft->harvest ($type);
    $self->{'oai'}->log ('i', 'done');
    return (1);
}

sub update
{
    my ($self, $rec) = @_;

    $self->{'db'}->update ('records', 'id', $rec);
}

1;

