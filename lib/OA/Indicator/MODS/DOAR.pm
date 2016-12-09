package OA::Indicator::MODS::DOAR;

use strict;
use warnings;
use OA::Indicator::XML;

sub new
{
    my ($class) = @_;
    my $self = {};

    $self->{'primary_fields'} = [qw(name type domain proposer)];
    $self->{'xpath'} = {
        id         => '/m:mods/m:identifier[@type="ds.dtic.dk:id:pub:dads:recordid"]',
        name       => '/m:mods/m:extension/s:snippet/s:directory/s:name',
        type       => '/m:mods/m:extension/s:snippet/s:directory/s:type',
        domain     => '/m:mods/m:extension/s:snippet/s:directory/s:domain',
        proposer   => '/m:mods/m:extension/s:snippet/s:directory/s:proposer',
    };
    $self->{'xml'} = new OA::Indicator::XML (m => 'http://www.loc.gov/mods/v3', s => 'http://dtic.dk/ds/snippet');
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

