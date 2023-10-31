select
 '&uttrekksdato' as tid_id,
 ku.maletidspunkt_kode,
 ku.bk_sb1_selskap_id,
 ku.sk_bankkunde_biii_id,
 ku.kredittforetak_flagg,
 ku.syndikat_flagg,
 ku.eierbytte_flagg,
 ku.rk_trekkonto_utenfor_lgd_flagg,
 ku.korr_kilde_trans_flagg,
 ku.korr_kilde_gl_flagg,
 ku.korr_kilde_konflikt_flagg,
 ku.ekskludert_konto_flagg,
 --ku.annet_spesielt,
 case when ku.korr_kilde_konflikt_flagg = '0' and (ku.korr_kilde_trans_flagg = '1' or ku.korr_kilde_gl_flagg = '1')
        then case when ku.korr_kilde_gl_flagg = '1'
                   and ku.sak_start_dato >= gl.trans_fom_dato
                   and ku.beregn_til_dato <= gl.trans_tom_dato then 'GL'
                  else 'TRANS'
              end
      else case when ku.sak_start_dato >= gl.trans_fom_dato
                 and ku.beregn_til_dato <= gl.trans_tom_dato
                 and ku.kredittforetak_flagg = '0'
                 and ku.syndikat_flagg = '0' then 'GL'
                else 'TRANS'
            end
  end kontantstrom_kilde_kode,
  '&batch_navn' as batch_navn
  from (select ku.sk_bankkunde_biii_id,
               ku.maletidspunkt_kode,
               ku.bk_sb1_selskap_id,
               ku.sak_start_dato,
               max(ku.beregn_til_dato) beregn_til_dato,
               max(ku.kredittforetak_flagg) kredittforetak_flagg,
               max(ku.syndikat_flagg) syndikat_flagg,
               max(ku.eierbytte_flagg) eierbytte_flagg,
               max(ku.rk_trekkonto_utenfor_lgd_flagg) rk_trekkonto_utenfor_lgd_flagg,
               max(ku.korr_kilde_trans_flagg) korr_kilde_trans_flagg,
               max(ku.korr_kilde_gl_flagg) korr_kilde_gl_flagg,
               case when max(ku.korr_kilde_trans_flagg) || max(ku.korr_kilde_gl_flagg) = '11' then '1' else '0' end korr_kilde_konflikt_flagg,
               max(ku.ekskludert_konto_flagg) ekskludert_konto_flagg
              -- listagg(nullif(spesielt, 'x'), ', ') within group (order by spesielt) annet_spesielt
                from (select ku.sk_bankkunde_biii_id,
                             ku.maletidspunkt_kode,
                             ku.bk_sb1_selskap_id,
                             ku.sak_start_dato,
                             max(ku.beregn_til_dato) beregn_til_dato,
                             max(ku.kredittforetak_flagg) kredittforetak_flagg,
                             max(ku.syndikat_flagg) syndikat_flagg,
                             max(ku.eierbytte_flagg) eierbytte_flagg,
                             max(ku.rk_trekkonto_utenfor_lgd_flagg) rk_trekkonto_utenfor_lgd_flagg,
                             max(ku.korr_kilde_trans_flagg) korr_kilde_trans_flagg,
                             max(ku.korr_kilde_gl_flagg) korr_kilde_gl_flagg,
                             max(ku.ekskludert_konto_flagg) ekskludert_konto_flagg
                        --     trim(x.column_value) spesielt
                        from RISIKO.LGD.m_d_bankkonto_biii_flagg_t ku
                        --     xmltable(('"' || nvl(replace(ku.annet_spesielt, ',', '","'), 'x') || '"')) x

                        where ku.tid_id = '&uttrekksdato'
                          and ku.batch_navn = '&batch_navn'
                          and (ku.maletidspunkt_kode,ku.bk_sb1_selskap_id) in (select maletidspunkt_kode, bk_sb1_selskap_id from RISIKO.LGD.M_KONFIGURASJON_BANK )
                       group by ku.sk_bankkunde_biii_id, ku.maletidspunkt_kode, ku.bk_sb1_selskap_id, ku.sak_start_dato --, trim(x.column_value)
                     ) ku
               group by ku.sk_bankkunde_biii_id, ku.maletidspunkt_kode, ku.bk_sb1_selskap_id, ku.sak_start_dato
        ) ku
  left join RISIKO.LGD.m_gl_periode_t gl on gl.gl_bankkode = ku.bk_sb1_selskap_id and gl.tid_id = '&uttrekksdato' and gl.batch_navn = '&batch_navn' and gl.maletidspunkt_kode = ku.maletidspunkt_kode