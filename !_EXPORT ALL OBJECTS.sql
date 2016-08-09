BEGIN
    FOR r_payment IN (SELECT *
                        FROM (SELECT id
                                FROM rk_rozr_nal_dokumenty
                               WHERE zaplaty > 0)
                       WHERE ROWNUM < 30)
    LOOP
        jg_obop_def.add_operation (p_object_id        => r_payment.id,
                                   p_object_type      => 'INVOICES_PAYMENTS',
                                   p_operation_type   => 'UPDATE');
    END LOOP;
END;
/

BEGIN
    FOR r_inma IN (SELECT id FROM ap_indeksy_materialowe)
    LOOP
        jg_obop_def.add_operation (p_object_id        => r_inma.id,
                                   p_object_type      => 'COMMODITIES',
                                   p_operation_type   => 'INSERT');
    END LOOP;
END;
/

BEGIN
    FOR r_sinv IN (SELECT *
                     FROM (  SELECT id
                               FROM lg_sal_invoices sinv
                              WHERE sinv.approved = 'T'
                           ORDER BY id DESC)
                    WHERE ROWNUM < 50)
    LOOP
        jg_obop_def.add_operation (p_object_id        => r_sinv.id,
                                   p_object_type      => 'INVOICES',
                                   p_operation_type   => 'INSERT');
    END LOOP;
END;
/

BEGIN
    FOR r_reze
        IN (SELECT *
              FROM (  SELECT reze.id
                        FROM lg_rzm_rezerwacje reze,
                             lg_rzm_zadania_rezerwacji zare
                       WHERE zare.id = reze.zare_id AND zare.zrre_typ = 'ZASI'
                    ORDER BY reze.id DESC)
             WHERE ROWNUM < 50)
    LOOP
        jg_obop_def.add_operation (p_object_id        => r_reze.id,
                                   p_object_type      => 'RESERVATIONS',
                                   p_operation_type   => 'UPDATE');
    END LOOP;
END;
/

BEGIN
    FOR r_grk IN (SELECT konr_id
                    FROM lg_kontrahenci_grup kogr)
    LOOP
        FOR r_osol
            IN (SELECT osol.id
                  FROM lg_osoby_log osol,
                       (    SELECT *
                              FROM lg_grupy_kontrahentow
                        START WITH id = 63
                        CONNECT BY PRIOR id = grkn_id) grko,
                       lg_kontrahenci_grup kngr
                 WHERE     osol.atrybut_t01 = grko.nazwa
                       AND grko.id = kngr.grkn_id
                       AND osol.aktualna = 'T'
                       AND kngr.konr_id IN (r_grk.konr_id))
        LOOP
            jg_obop_def.add_operation (
                p_object_id        => r_osol.id,
                p_object_type      => 'SALES_REPRESENTATIVES',
                p_operation_type   => 'INSERT');
        END LOOP;
    END LOOP;
END;
/

BEGIN
    FOR r_set IN (SELECT kpl_inma_id FROM lg_kpl_skladniki_kompletu)
    LOOP
        jg_obop_def.add_operation (p_object_id        => r_set.kpl_inma_id,
                                   p_object_type      => 'SETS_COMPONENTS',
                                   p_operation_type   => 'UPDATE');
    END LOOP;
END;
/

BEGIN
    FOR r_spdo IN (SELECT id FROM ap_sposoby_dostaw)
    LOOP
        jg_obop_def.add_operation (p_object_id        => r_spdo.id,
                                   p_object_type      => 'DELIVERY_METHODS',
                                   p_operation_type   => 'UPDATE');
    END LOOP;
END;
/

BEGIN
    FOR r_foza IN (SELECT id FROM ap_formy_zaplaty)
    LOOP
        jg_obop_def.add_operation (p_object_id        => r_foza.id,
                                   p_object_type      => 'PAYMENTS_METHODS',
                                   p_operation_type   => 'UPDATE');
    END LOOP;
END;
/

BEGIN
    FOR r_disc IN (SELECT *
                     FROM (  SELECT id
                               FROM lg_przyp_upustow
                           ORDER BY id DESC)
                    WHERE ROWNUM < 50)
    LOOP
        jg_obop_def.add_operation (p_object_id        => r_disc.id,
                                   p_object_type      => 'DISCOUNTS',
                                   p_operation_type   => 'UPDATE');
    END LOOP;
END;
/

BEGIN
    FOR r_konr IN (SELECT id FROM ap_kontrahenci)
    LOOP
        jg_obop_def.add_operation (p_object_id        => r_konr.id,
                                   p_object_type      => 'CONTRACTORS',
                                   p_operation_type   => 'INSERT');
    END LOOP;
END;
/

BEGIN
    FOR r_stma IN (SELECT id
                     FROM ap_stany_magazynowe
                    WHERE stan_goracy > 0)
    LOOP
        jg_obop_def.add_operation (p_object_id        => r_stma.id,
                                   p_object_type      => 'WAREHOUSES',
                                   p_operation_type   => 'UPDATE');
    END LOOP;
END;
/

BEGIN
    FOR r_umsp IN (SELECT *
                     FROM (  SELECT id
                               FROM lg_ums_umowy_sprz
                           ORDER BY id DESC)
                    WHERE ROWNUM < 20)
    LOOP
        jg_obop_def.add_operation (p_object_id        => r_umsp.id,
                                   p_object_type      => 'CONTRACTS',
                                   p_operation_type   => 'INSERT');
    END LOOP;
END;
/


BEGIN
    FOR r_doob IN (SELECT *
                     FROM (  SELECT id
                               FROM ap_dokumenty_obrot
                           ORDER BY id DESC)
                    WHERE ROWNUM < 100)
    LOOP
        jg_obop_def.add_operation (p_object_id        => r_doob.id,
                                   p_object_type      => 'DELIVERIES',
                                   p_operation_type   => 'INSERT');
    END LOOP;
END;
/

BEGIN
    FOR r_konr
        IN (SELECT *
              FROM (  SELECT konr.id
                        FROM (SELECT CASE
                                         WHEN konr.atrybut_t05 LIKE '%UM IND%'
                                         THEN
                                             'T'
                                         ELSE
                                             'N'
                                     END
                                         individual_contract,
                                     konr.*
                                FROM ap_kontrahenci konr) konr,
                             lg_przyp_upustow prup
                       WHERE prup.grod_id(+) = konr.grod_id
                    ORDER BY prup.id DESC)
             WHERE ROWNUM < 100)
    LOOP
        jg_obop_def.add_operation (p_object_id        => r_konr.id,
                                   p_object_type      => 'TRADE_CONTRACTS',
                                   p_operation_type   => 'INSERT');
    END LOOP;
END;
/


BEGIN
    FOR r_konr
        IN (SELECT *
              FROM (  SELECT konr.id
                        FROM (SELECT CASE
                                         WHEN konr.atrybut_t05 LIKE '%UM IND%'
                                         THEN
                                             'T'
                                         ELSE
                                             'N'
                                     END
                                         individual_contract,
                                     konr.*
                                FROM ap_kontrahenci konr) konr,
                             lg_przyp_upustow prup
                       WHERE prup.grod_id(+) = konr.grod_id
                    ORDER BY prup.id)
             WHERE ROWNUM < 100)
    LOOP
        jg_obop_def.add_operation (
            p_object_id        => r_konr.id,
            p_object_type      => 'TRADE_CONTRACTS_INDIVIDUAL',
            p_operation_type   => 'INSERT');
    END LOOP;
END;
/


BEGIN
    FOR r_konr
        IN (SELECT *
              FROM (  SELECT konr.id
                        FROM (SELECT CASE
                                         WHEN konr.atrybut_t05 LIKE '%UM IND%'
                                         THEN
                                             'T'
                                         ELSE
                                             'N'
                                     END
                                         individual_contract,
                                     konr.*
                                FROM ap_kontrahenci konr) konr,
                             lg_przyp_upustow prup
                       WHERE prup.grod_id(+) = konr.grod_id
                    ORDER BY prup.id)
             WHERE ROWNUM < 100)
    LOOP
        jg_obop_def.add_operation (
            p_object_id        => r_konr.id,
            p_object_type      => 'TRADE_CONTRACTS_INDIVIDUAL',
            p_operation_type   => 'INSERT');
    END LOOP;
END;
/


BEGIN
    FOR r_konr
        IN (SELECT *
              FROM (  SELECT puko.id
                        FROM lg_plo_punkty_kontrahenta puko,
                             ap_kontrahenci konr
                       WHERE konr.id = puko.konr_id
                    ORDER BY puko.id)
             WHERE ROWNUM < 100)
    LOOP
        jg_obop_def.add_operation (p_object_id        => r_konr.id,
                                   p_object_type      => 'LOYALITY_POINTS',
                                   p_operation_type   => 'INSERT');
    END LOOP;
END;
/

BEGIN
    FOR r_konr
        IN (SELECT *
              FROM (  SELECT fwk_changed.id
                        FROM jbs_mp_przeglad_fwk fwk,
                             jbs_mp_przeglad_fwk fwk_changed
                       WHERE     fwk.konr_symbol = fwk_changed.konr_symbol
                             AND fwk.data_faktury >=
                                     ADD_MONTHS (TRUNC (SYSDATE, 'MM'), -12)
                    GROUP BY fwk_changed.id
                    ORDER BY id DESC)
             WHERE ROWNUM < 100)
    LOOP
        jg_obop_def.add_operation (p_object_id        => r_konr.id,
                                   p_object_type      => 'SUPPORT_FUNDS',
                                   p_operation_type   => 'INSERT');
    END LOOP;
END;
/
