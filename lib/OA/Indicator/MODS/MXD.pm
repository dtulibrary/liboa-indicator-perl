package OA::Indicator::MODS::MXD;

use strict;
use warnings;
use OA::Indicator::XML;

sub new
{
    my ($class) = @_;
    my $self = {};

    $self->{'primary_fields'} = [qw(source_id year type level review)];
    $self->{'xpath'} = {
        source_id  => '/m:mods/m:identifier[@type="ds.dtic.dk:id:pub:dads:sourceid"]',
        year       => '/m:mods/m:relatedItem/m:originInfo/m:dateOther',
        type       => '/m:mods/m:genre[@type="ds.dtic.dk:type:origin"]',
        level      => '/m:mods/m:targetAudience[@authorityURI="ds.dtic.dk:level:origin"]',
        review     => '/m:mods/m:targetAudience[@authorityURI="ds.dtic.dk:review:origin"]',
        url        => '/m:mods/m:location/m:url',
        url_access => '@access',
        url_note   => '@note',
        pissn      => '//m:identifier[@type="ds.dtic.dk:id:pub:dads:pissn"]',
        eissn      => '//m:identifier[@type="ds.dtic.dk:id:pub:dads:eissn"]',
    };
    $self->{'xml'} = new OA::Indicator::XML (m => 'http://www.loc.gov/mods/v3');
    return (bless ($self, $class));
}

sub parse
{
    my ($self, $xml) = @_;

    $self->{'doc'} = $self->{'xml'}->parse ($xml);
    $self->{'rec'} = {};
    foreach my $f (qw(source_id year)) {
        $self->{'rec'}{$f} = $self->{'xml'}->field ($self->{'doc'}, $self->{'xpath'}{$f});
    }
}

sub id
{
    my ($self) = @_;

    return ($self->{'rec'}{'source_id'});
}

sub year
{
    my ($self) = @_;

    return ($self->{'rec'}{'year'});
}

sub primary_fields
{
    my ($self) = @_;

    return (@{$self->{'primary_fields'}});
}

sub primary
{
    my ($self) = @_;
    my @ret = ();
    foreach my $f (@{$self->{'primary_fields'}}) {
        if (!exists ($self->{'rec'}{$f})) {
            $self->{'rec'}{$f} = $self->{'xml'}->field ($self->{'doc'}, $self->{'xpath'}{$f});
        }
        push (@ret, $self->{'rec'}{$f});
    }
    return (@ret);
}

sub fulltext_fields
{
    return ('access', 'url');
}

sub fulltext
{
    my ($self) = @_;
    my @ret = ();

    foreach my $obj ($self->{'xml'}->node ($self->{'doc'}, $self->{'xpath'}{'url'})) {
        if ($self->{'xml'}->field ($obj, $self->{'xpath'}{'url_note'}) eq 'ds.dtic.dk:link:remote_fulltext') {
            my $access = $self->{'xml'}->field ($obj, $self->{'xpath'}{'url_access'});
            my $url = $self->{'xml'}->field ($obj, '.');
            push (@ret, [$access, $url]);
        }
    }
    return (@ret);
}

sub issn_fields
{
    return ('type', 'issn');
}

sub issn
{
    my ($self) = @_;
    my @ret = ();

    foreach my $issn ($self->{'xml'}->field ($self->{'doc'}, $self->{'xpath'}{'pissn'})) {
        push (@ret, ['print', $issn]);
    }
    foreach my $issn ($self->{'xml'}->field ($self->{'doc'}, $self->{'xpath'}{'eissn'})) {
        push (@ret, ['electronic', $issn]);
    }
    return (@ret);
}

1;

