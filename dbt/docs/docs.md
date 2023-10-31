{% docs docs_v_d_bankkunde_biii_9mnd %}

# 9mnd model

### Om tabellen
Denne modelen/view sammenstiller saker fra samme kunde som har mindre enn 9 måneder forskjell mellom avsluttet dato på den eldste og saks start dato på den nyeste, iht til EBA rettningslinjer.

### Datakilder
Denne tabellen henter data fra:
 - f_lgd_bankkunde_biii_t: dette er måltabellen for skyggeimplementeringen av misligholdsmodulen. Tabellen her både SCD1 og SCD2 håndteres i overføringen til LGD schemaet.
 

{% enddocs %}