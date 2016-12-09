package OA::Indicator::MODS::JWlep;

use strict;
use warnings;
use OA::Indicator::XML;

sub new
{
    my ($class) = @_;
    my $self = {};

    $self->{'primary_fields'} = [qw(pissn eissn title publisher holding url)];
    $self->{'xpath'} = {
        id         => '/m:mods/m:identifier[@type="ds.dtic.dk:id:pub:dads:recordid"]',
        pissn      => '//m:identifier[@type="ds.dtic.dk:id:pub:dads:pissn"]',
        eissn      => '//m:identifier[@type="ds.dtic.dk:id:pub:dads:eissn"]',
        title      => '/m:mods/m:titleInfo/m:title',
        publisher  => '/m:mods/m:relatedItem/m:originInfo/m:publisher',
        holding    => '/m:mods/m:location/m:holdingExternal/holdings/holding/holdingStructured/set/completeness',
        url        => '/m:mods/m:location/m:url',
    };
    $self->{'xml'} = new OA::Indicator::XML (m => 'http://www.loc.gov/mods/v3');
    return (bless ($self, $class));
}

sub parse
{
    my ($self, $xml) = @_;

    $self->{'doc'} = $self->{'xml'}->parse ($xml);
    $self->{'rec'} = {};
}

sub field
{
    my ($self, $name, $doc) = @_;

    my $s = $self->{'xml'}->field ($self->{'xpath'}{$name}, $doc);
    $s =~ s/[\s\t\n\r]+/ /g;
    $s =~ s/^\s//;
    $s =~ s/\s$//;
    return ($s);
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

