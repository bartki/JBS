

CREATE OR REPLACE TRIGGER jg_adge_observe
    BEFORE INSERT OR
           DELETE OR
           UPDATE OF nr_domu,
                     miejscowosc,
                     ulica,
                     nr_lokalu
    ON pa_adr_adresy_geograficzne
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
DECLARE
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    IF pa_wass_def.wartosc (p_nazwa => 'IMPORT_INFINITE') = 'T'
    THEN
        RETURN;
    END IF;

    FOR r_adre IN (SELECT konr_id
                   FROM pa_adr_adresy_kontrahentow_vw
                   WHERE adge_id = :new.id)
    LOOP
        jg_obop_def.add_operation (p_object_id        => r_adre.konr_id,
                                   p_object_type      => 'CONTRACTORS',
                                   p_operation_type   => 'UPDATE');
    END LOOP;

    COMMIT;
END;
/

CREATE OR REPLACE TRIGGER jg_attachments_observe
    BEFORE INSERT OR UPDATE
    ON pa_attachments
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF pa_atki_agd.code (p_id => :new.atki_id) IN ('KONTRAKT')
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'CONTRACT_ATTACHMENT',
                                   p_operation_type   => 'UPDATE',
                                   p_attachment       => 'T');
    END IF;

    IF pa_atki_agd.code (p_id => :new.atki_id) IN ('INMTR_DKMT')
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'COMMODITY_ATTACHMENT',
                                   p_operation_type   => 'UPDATE',
                                   p_attachment       => 'T');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_cezb_observe
    BEFORE INSERT OR
           DELETE OR
           UPDATE OF konr_id,
                     rcez_id,
                     cena,
                     jdmr_nazwa,
                     grod_id,
                     gras_id,
                     typ,
                     inma_id,
                     cena_brutto
    ON ap_ceny_zbytu
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF :new.inma_id IS NOT NULL
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.inma_id,
                                   p_object_type      => 'COMMODITIES',
                                   p_operation_type   => 'UPDATE');
    ELSIF :old.inma_id IS NOT NULL
    THEN
        jg_obop_def.add_operation (p_object_id        => :old.inma_id,
                                   p_object_type      => 'COMMODITIES',
                                   p_operation_type   => 'UPDATE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_deliveries_observe
    BEFORE INSERT OR UPDATE OF wskaznik_zatwierdzenia
    ON ap_dokumenty_obrot
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF     NVL (:old.wskaznik_zatwierdzenia, 'N') = 'N'
       AND :new.wskaznik_zatwierdzenia = 'T'
       AND :new.wzty_kod IN ('WZ')
       AND :new.numer_zamowienia IS NOT NULL
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'DELIVERIES',
                                   p_operation_type   => 'UPDATE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_discounts_observe
    BEFORE INSERT OR
           DELETE OR
           UPDATE OF data_od,
                     data_do,
                     inma_id,
                     grod_id,
                     upust_procentowy,
                     konr_id,
                     gras_id
    ON lg_przyp_upustow
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF INSERTING OR UPDATING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'DISCOUNTS',
                                   p_operation_type   => 'UPDATE');
    ELSIF DELETING
    THEN
        jg_obop_def.add_operation (p_object_id        => :old.id,
                                   p_object_type      => 'DISCOUNTS',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_foza_observe
    BEFORE INSERT OR DELETE OR UPDATE OF opis, typ
    ON ap_formy_zaplaty
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF INSERTING OR UPDATING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'PAYMENTS_METHODS',
                                   p_operation_type   => 'UPDATE');
    ELSIF DELETING
    THEN
        jg_obop_def.add_operation (p_object_id        => :old.id,
                                   p_object_type      => 'PAYMENTS_METHODS',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_grin_observe
    BEFORE INSERT OR DELETE OR UPDATE OF podstawowa, inma_id
    ON ap_grupy_indeksow
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF :new.inma_id IS NOT NULL
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.inma_id,
                                   p_object_type      => 'COMMODITIES',
                                   p_operation_type   => 'UPDATE');
    ELSIF :old.inma_id IS NOT NULL
    THEN
        jg_obop_def.add_operation (p_object_id        => :old.inma_id,
                                   p_object_type      => 'COMMODITIES',
                                   p_operation_type   => 'UPDATE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_inma_observe
    BEFORE INSERT OR
           DELETE OR
           UPDATE OF jdmr_nazwa,
                     stva_id,
                     cecha,
                     nazwa,
                     id,
                     indeks
    ON ap_indeksy_materialowe
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF INSERTING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'COMMODITIES',
                                   p_operation_type   => 'INSERT');
    ELSIF UPDATING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'COMMODITIES',
                                   p_operation_type   => 'UPDATE');
    ELSIF DELETING
    THEN
        jg_obop_def.add_operation (p_object_id        => :old.id,
                                   p_object_type      => 'COMMODITIES',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_invoice_eksport_trg
    BEFORE INSERT OR DELETE OR UPDATE
    ON lg_sal_invoices
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    --  ASSERT(FALSE,:NEW.DOC_TYPE||'#'|| :NEW.approved);
    IF :new.doc_type IN ('FS',
                         'KS',
                         'FE',
                         'KE')
    THEN
        IF     :new.approved = 'T'
           AND NVL (:old.approved, 'N') = 'N'
           AND (INSERTING OR UPDATING)
        THEN
            jg_obop_def.add_operation (p_object_id        => :new.id,
                                       p_object_type      => 'INVOICES',
                                       p_operation_type   => 'INSERT');
        END IF;
    END IF;

    IF DELETING
    THEN
        jg_obop_def.add_operation (p_object_id        => :old.id,
                                   p_object_type      => 'INVOICES',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_kngr_observe
    BEFORE INSERT OR DELETE OR UPDATE OF grkn_id, konr_id, id
    ON lg_kontrahenci_grup
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
DECLARE
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    FOR r_osol
        IN (SELECT osol.id
            FROM lg_osoby_log osol,
                 (SELECT *
                    FROM lg_grupy_kontrahentow
                  START WITH id = 63
                  CONNECT BY PRIOR id = grkn_id) grko
            WHERE     osol.atrybut_t01 = grko.nazwa
                  AND grko.id IN (:new.grkn_id, :old.grkn_id))
    LOOP
        jg_obop_def.add_operation (p_object_id        => r_osol.id,
                                   p_object_type      => 'SALES_REPRESENTATIVES',
                                   p_operation_type   => 'INSERT');
    END LOOP;


    FOR r_osol
        IN (SELECT osol.id
            FROM lg_osoby_log osol,
                 (SELECT *
                    FROM lg_grupy_kontrahentow
                  START WITH id = 63
                  CONNECT BY PRIOR id = grkn_id) grko,
                 lg_kontrahenci_grup kngr
            WHERE     osol.atrybut_t01 = grko.nazwa
                  AND grko.id = kngr.grkn_id
                  AND osol.aktualna = 'T'
                  AND kngr.konr_id IN (:new.konr_id, :old.konr_id))
    LOOP
        jg_obop_def.add_operation (p_object_id        => r_osol.id,
                                   p_object_type      => 'SALES_REPRESENTATIVES',
                                   p_operation_type   => 'INSERT');
    END LOOP;

    COMMIT;
END;
/

CREATE OR REPLACE TRIGGER jg_konr_observe
    BEFORE INSERT OR
           DELETE OR
           UPDATE OF id,
                     aktualny,
                     symbol,
                     skrot,
                     odbiorca,
                     mail,
                     nazwa,
                     nr_umowy_ind,
                     platnik,
                     platnik_id,
                     nr_tel,
                     nip,
                     nr_faksu,
                     blokada_sprz,
                     dni_do_zaplaty
    ON ap_kontrahenci
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF pa_wass_def.wartosc (p_nazwa => 'IMPORT_INFINITE') = 'T'
    THEN
        RETURN;
    END IF;

    IF INSERTING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'CONTRACTORS',
                                   p_operation_type   => 'INSERT');
    ELSIF UPDATING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'CONTRACTORS',
                                   p_operation_type   => 'UPDATE');
    ELSIF DELETING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'CONTRACTORS',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_likr_observe
    BEFORE INSERT OR
           DELETE OR
           UPDATE OF data_do,
                     wartosc,
                     data_od,
                     konr_id
    ON lg_knr_limity_kredyt
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF pa_wass_def.wartosc (p_nazwa => 'IMPORT_INFINITE') = 'T'
    THEN
        RETURN;
    END IF;

    IF INSERTING OR UPDATING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.konr_id,
                                   p_object_type      => 'CONTRACTORS',
                                   p_operation_type   => 'UPDATE');
    ELSIF DELETING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.konr_id,
                                   p_object_type      => 'CONTRACTORS',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_loyality_points_observe
    BEFORE INSERT OR DELETE OR UPDATE
    ON lg_plo_punkty_kontrahenta
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF INSERTING OR UPDATING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'LOYALITY_POINTS',
                                   p_operation_type   => 'UPDATE');
    ELSIF DELETING
    THEN
        jg_obop_def.add_operation (p_object_id        => :old.id,
                                   p_object_type      => 'LOYALITY_POINTS',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_order_patterns
    BEFORE INSERT OR UPDATE
    ON lg_documents_templates
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF :new.document_type IN ('ZS', 'ZE')
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'ORDERS_PATTERNS',
                                   p_operation_type   => 'UPDATE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_osol_observe
    BEFORE INSERT OR
           DELETE OR
           UPDATE OF aktualna,
                     first_name,
                     id,
                     surname,
                     kod,
                     atrybut_t01
    ON lg_osoby_log
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF INSERTING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'SALES_REPRESENTATIVES',
                                   p_operation_type   => 'INSERT');
    ELSIF UPDATING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'SALES_REPRESENTATIVES',
                                   p_operation_type   => 'UPDATE');
    ELSIF DELETING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'SALES_REPRESENTATIVES',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_prje_observe
    BEFORE INSERT OR
           DELETE OR
           UPDATE OF id,
                     inma_id,
                     jdmr_nazwa,
                     kod_kreskowy
    ON lg_przeliczniki_jednostek
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    jg_obop_def.add_operation (p_object_id        => :new.inma_id,
                               p_object_type      => 'COMMODITIES',
                               p_operation_type   => 'UPDATE');
END;
/

CREATE OR REPLACE TRIGGER jg_reze_observe
    BEFORE INSERT OR DELETE OR UPDATE
    ON lg_rzm_rezerwacje
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF pa_wass_def.wartosc (p_nazwa => 'IMPORT_INFINITE') = 'T'
    THEN
        RETURN;
    END IF;
    
    IF lg_rzm_zare_agd.zrre_typ (p_id => NVL (:new.zare_id, :old.zare_id)) =
           'ZASI'
    THEN
        IF INSERTING OR UPDATING
        THEN
            jg_obop_def.add_operation (p_object_id        => :new.id,
                                       p_object_type      => 'RESERVATIONS',
                                       p_operation_type   => 'UPDATE');
        ELSIF DELETING
        THEN
            jg_obop_def.add_operation (p_object_id        => :old.id,
                                       p_object_type      => 'RESERVATIONS',
                                       p_operation_type   => 'DELETE');
        END IF;
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_rond_observe
    BEFORE INSERT OR DELETE OR UPDATE
    ON rk_rozr_nal_dokumenty
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF     (INSERTING OR UPDATING)
       AND :new.pozostalo_do_zaplaty_z_kor != :old.pozostalo_do_zaplaty_z_kor
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'INVOICES_PAYMENTS',
                                   p_operation_type   => 'UPDATE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_sets_observe
    BEFORE INSERT OR DELETE OR UPDATE
    ON lg_kpl_skladniki_kompletu
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF INSERTING OR UPDATING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.kpl_inma_id,
                                   p_object_type      => 'SETS_COMPONENTS',
                                   p_operation_type   => 'UPDATE');
    ELSIF DELETING
    THEN
        jg_obop_def.add_operation (p_object_id        => :old.kpl_inma_id,
                                   p_object_type      => 'SETS_COMPONENTS',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_sinv_observe
    BEFORE INSERT OR DELETE OR UPDATE
    ON lg_sal_invoices
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF     (INSERTING OR UPDATING)
       AND :new.approved = 'T'
       AND NVL (:old.approved, 'N') = 'N'
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'INVOICES',
                                   p_operation_type   => 'INSERT');
    ELSIF    (DELETING)
          OR     (UPDATING)
             AND :old.approved = 'T'
             AND NVL (:new.approved, 'N') = 'N'
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'INVOICES',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_spdo_observe
    BEFORE INSERT OR DELETE OR UPDATE OF opis, transport_wlasny
    ON ap_sposoby_dostaw
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF INSERTING OR UPDATING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'DELIVERY_METHODS',
                                   p_operation_type   => 'UPDATE');
    ELSIF DELETING
    THEN
        jg_obop_def.add_operation (p_object_id        => :old.id,
                                   p_object_type      => 'DELIVERY_METHODS',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_support_fund_observe
    BEFORE INSERT OR DELETE OR UPDATE
    ON lg_sal_invoices
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF INSERTING OR UPDATING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'SUPPORT_FUNDS',
                                   p_operation_type   => 'UPDATE');
    ELSIF DELETING
    THEN
        jg_obop_def.add_operation (p_object_id        => :old.id,
                                   p_object_type      => 'SUPPORT_FUNDS',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_trade_contracts_observe
    BEFORE INSERT OR
           UPDATE OF atrybut_n04,
                     dni_do_zaplaty,
                     atrybut_n05,
                     foza_kod,
                     atrybut_n03,
                     atrybut_n02,
                     atrybut_t07,
                     limit_kredytowy
    ON ap_kontrahenci
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF :new.atrybut_t05 IS NULL
    THEN
        RETURN;
    END IF;

    IF INSERTING OR UPDATING
    THEN
        IF :new.atrybut_t05 LIKE '%UM IND%'
        THEN
            jg_obop_def.add_operation (
                p_object_id        => :new.id,
                p_object_type      => 'TRADE_CONTRACTS_INDIVIDUAL',
                p_operation_type   => 'UPDATE');
        ELSE
            jg_obop_def.add_operation (p_object_id        => :new.id,
                                       p_object_type      => 'TRADE_CONTRACTS',
                                       p_operation_type   => 'UPDATE');
        END IF;
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_umsp_observe
    BEFORE INSERT OR DELETE OR UPDATE
    ON lg_ums_umowy_sprz
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF     (INSERTING OR UPDATING)
       AND :new.zatwierdzona = 'T'
       AND NVL (:old.zatwierdzona, 'N') = 'N'
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'CONTRACTS',
                                   p_operation_type   => 'INSERT');
    ELSIF    (DELETING)
          OR     (UPDATING)
             AND :old.zatwierdzona = 'T'
             AND NVL (:new.zatwierdzona, 'N') = 'N'
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'CONTRACTS',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_uzad_observe
    BEFORE INSERT OR DELETE OR UPDATE
    ON lg_pdm_uzycia_adresow
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
DECLARE
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    IF pa_wass_def.wartosc (p_nazwa => 'IMPORT_INFINITE') = 'T'
    THEN
        RETURN;
    END IF;

    FOR r_adre IN (SELECT konr_id
                   FROM pa_adr_adresy_kontrahentow_vw
                   WHERE uzad_id = :new.id)
    LOOP
        jg_obop_def.add_operation (p_object_id        => r_adre.konr_id,
                                   p_object_type      => 'CONTRACTORS',
                                   p_operation_type   => 'UPDATE');
    END LOOP;

    COMMIT;
END;
/

CREATE OR REPLACE TRIGGER jg_wace_observe
    BEFORE INSERT OR
           DELETE OR
           UPDATE OF data_od,
                     data_do,
                     price_min_net,
                     jdmr_nazwa,
                     price_min_gross,
                     inma_id
    ON lg_wah_warunki_cen
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF :new.inma_id IS NOT NULL
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.inma_id,
                                   p_object_type      => 'COMMODITIES',
                                   p_operation_type   => 'UPDATE');
    ELSIF :old.inma_id IS NOT NULL
    THEN
        jg_obop_def.add_operation (p_object_id        => :old.inma_id,
                                   p_object_type      => 'COMMODITIES',
                                   p_operation_type   => 'UPDATE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_warehouse_observe
    BEFORE INSERT OR UPDATE OF stan_goracy
    ON ap_stany_magazynowe
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    jg_obop_def.add_operation (p_object_id        => :new.id,
                               p_object_type      => 'WAREHOUSES',
                               p_operation_type   => 'UPDATE');
END;
/
