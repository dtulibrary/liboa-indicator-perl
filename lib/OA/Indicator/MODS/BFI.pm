package OA::Indicator::MODS::BFI;

use strict;
use warnings;
use OA::Indicator::XML;

sub new
{
    my ($class) = @_;
    my $self = {};

    $self->{'primary_fields'} = [qw(source source_id pubid doc_type doc_review doc_level class research level fraction point cooperation title lang issn eissn)];
    $self->{'xpath'} = {
        id         => '/m:mods/m:identifier[@type="ds.dtic.dk:id:pub:dads:recordid"]',
        year       => '/m:mods/m:relatedItem/m:originInfo[@eventType="publisher"]/m:dateOther',
        source     => '/m:mods/m:extension/d:bfi/d:contributions/d:contribution/d:recordOriginOrig',
        source_id  => '/m:mods/m:extension/d:bfi/d:contributions/d:contribution/d:identifier[@type="ds.dtic.dk:id:pub:dads:sourceid"]',
        pubid      => '/m:mods/m:identifier[@type="ds.dtic.dk:id:pub:bfi:publication"]',
        doc_type   => '/m:mods/m:extension/d:bfi/d:contributions/d:contribution/d:doctype',
        doc_review => '/m:mods/m:extension/d:bfi/d:contributions/d:contribution/d:targetAudience[@authorityURI="ds.dtic.dk:review:origin"]',
        doc_level  => '/m:mods/m:extension/d:bfi/d:contributions/d:contribution/d:targetAudience[@authorityURI="ds.dtic.dk:level:origin"]',
        class      => '/m:mods/m:subject[@authorityURI="ds.dtic.dk:sub:uncontrolled"]/m:topic',
        research   => '/m:mods/m:subject[@authorityURI="ds.dtic.dk:sub:controlled"]/m:topic[@authorityURI="ds.dtic.dk:sub:bfi:bfi:code"]',
        level      => '/m:mods/m:identifier[@type="ds.dtic.dk:id:pub:bfi:level"]',
        fraction   => '/m:mods/m:extension/d:bfi/d:contributions/d:contribution/d:fraction',
        point      => '/m:mods/m:extension/d:bfi/d:contributions/d:contribution/d:points',
        cooperation=> '/m:mods/m:extension/d:bfi/d:contributions/d:contribution/d:cooperation',
        title      => '/m:mods/m:titleInfo/m:title',
        lang       => '/m:mods/m:language/m:languageTerm',
        issn       => '/m:mods/m:relatedItem/m:identifier[@type="ds.dtic.dk:id:pub:dads:pissn"]',
        eissn      => '/m:mods/m:relatedItem/m:identifier[@type="ds.dtic.dk:id:pub:dads:eissn"]',
    };
    $self->{'xml'} = new OA::Indicator::XML (m => 'http://www.loc.gov/mods/v3', d => 'http://dtic.dk/ds');
    return (bless ($self, $class));
}

sub parse
{
    my ($self, $xml) = @_;

    $self->{'doc'} = $self->{'xml'}->parse ($xml);
    $self->{'rec'} = {};
    foreach my $f (qw(year source_id)) {
        $self->{'rec'}{$f} = $self->field ($f);
    }
}

sub field
{
    my ($self, $name) = @_;

    my $s = $self->{'xml'}->field ($self->{'xpath'}{$name});
    $s =~ s/[\s\t\n\r]+/ /g;
    $s =~ s/^\s//;
    $s =~ s/\s$//;
    return ($s);
}

sub year
{
    my ($self) = @_;

    return ($self->{'rec'}{'year'});
}

sub id
{
    my ($self) = @_;

    return ($self->{'rec'}{'source_id'});
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
            $self->{'rec'}{$f} = $self->field ($f);
        }
        push (@ret, $self->{'rec'}{$f});
    }
    return (@ret);
}

1;

