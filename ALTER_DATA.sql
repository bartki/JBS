BEGIN
    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt)
             VALUES (
                        jg_sqre_seq.NEXTVAL,
                        'INVOICES_PAYMENTS',
                        'SELECT   rndo.symbol_dokumentu invoice_number,
         rndo.termin_platnosci due_date,
         rndo.forma_platnosci payment_form,         
         konr.symbol payer_symbol,
         konr.nazwa payer_name,
         rndo.wartosc_dok_z_kor_wwb total,
         rndo.poz_do_zaplaty_dok_z_kor_wwb amount_left,
         CURSOR (
             SELECT rnwp.symbol_dokumentu payment_doc_number,
                    rnwp.data_dokumentu payment_date,
                    rnwp.zaplata_wwb amount_paid
               FROM     rk_rozr_nal_dok_plat_rk_vw rnwp
              WHERE     rnwp.rndo_id = rndo.rndo_id
                    AND rnwp.zaplata_wwb is not null 
                    AND rnwp.typ = ''P'')
             payments_details
    FROM rk_rozr_nal_dokumenty_vw rndo, ap_kontrahenci konr
   WHERE     konr.id = rndo.konr_id
         AND rndo.rnwp_rnwp_id IS NULL
         AND rndo.typ in (''FAK'',''KOR'')        
         AND rndo.rndo_id in (:p_id)
GROUP BY rndo.symbol_dokumentu,
         rndo.termin_platnosci,
         rndo.forma_platnosci,
         konr.id,
         konr.symbol,
         konr.nazwa,
         rndo.wartosc_dok_z_kor_wwb,
         rndo.poz_do_zaplaty_dok_z_kor_wwb,
         rndo.rndo_id',
                        '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
        <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()" />
        </xsl:copy>
    </xsl:template>
    <xsl:template priority="2" match="ROW">
        <INVOICE_PAYMENTS><xsl:apply-templates/></INVOICE_PAYMENTS>            
    </xsl:template>
    <xsl:template priority="2" match="PAYMENTS_DETAILS/PAYMENTS_DETAILS_ROW">
        <PAYMENT_DETAIL><xsl:apply-templates/></PAYMENT_DETAIL>            
    </xsl:template>
</xsl:stylesheet>');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt)
             VALUES (
                        jg_sqre_seq.NEXTVAL,
                        'COMMODITIES',
                        'SELECT inma.indeks item_index,
       inma.nazwa name,
       inma.jdmr_nazwa base_unit_of_measure_code,       
       (SELECT MAX (kod_kreskowy) ean
          FROM lg_przeliczniki_jednostek prje
         WHERE     prje.kod_kreskowy IS NOT NULL
               AND prje.inma_id = inma.id
               AND prje.jdmr_nazwa = inma.jdmr_nazwa) base_ean_code,
       (SELECT rv_meaning
          FROM cg_ref_codes
         WHERE rv_domain = ''LG_CECHY_INMA'' AND rv_low_value = inma.cecha)
           TYPE,
       (SELECT stva.kod
          FROM rk_stawki_vat stva
         WHERE stva.id = inma.stva_id)
           vat_rate,
       CURSOR (SELECT jdmr_nazwa unit_of_measure_code, kod_kreskowy ean_code
                 FROM lg_przeliczniki_jednostek prje
                WHERE prje.inma_id = inma.id)
           units_of_measure,
       CURSOR (
           SELECT walu.kod currency,
                  cezb.cena net_price,
                  cezb.cena_brutto gross_price,
                  cezb.jdmr_nazwa unit_of_measure_COde,
                  rcez.rodzaj price_type
             FROM ap_ceny_zbytu cezb,
                  ap_rodzaje_ceny_zbytu rcez,
                  rk_waluty walu
            WHERE     cezb.rcez_id = rcez.id
                  AND cezb.typ = ''SPRZEDAZ''
                  AND cezb.grod_id IS NULL
                  AND cezb.gras_id IS NULL
                  AND cezb.konr_id IS NULL
                  AND walu.id = cezb.walu_id
                  AND cezb.sprzedaz = ''T''
                  AND lg_cezb_sql.aktualna_tn (cezb.id) = ''T''
                  AND cezb.inma_id = inma.id)
           prices,
       CURSOR (
           SELECT wace.price_min_net net_price,
                  wace.price_min_gross gross_price,
                  wace.jdmr_nazwa unit_of_measure_code
             FROM lg_wah_warunki_cen wace
            WHERE     wace.price_min_net IS NOT NULL
                  AND wace.price_min_gross IS NOT NULL
                  AND wace.data_od <= SYSDATE
                  AND (wace.data_do >= SYSDATE OR wace.data_do IS NULL)
                  AND wace.inma_id = inma.id)
           minimal_prices,
       CURSOR (SELECT gras.grupa_asortymentowa group_name, gras.kod group_code, grin.podstawowa is_primary
                 FROM ap_grupy_indeksow grin, ap_grupy_asortymentowe gras
                WHERE gras.id = grin.gras_id AND grin.inma_id = inma.id)
           groups
  FROM ap_indeksy_materialowe inma
 WHERE inma.id IN ( :p_id)',
                        '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
        <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()" />
        </xsl:copy>
    </xsl:template>
    <xsl:template priority="2" match="UNITS_OF_MEASURE/UNITS_OF_MEASURE_ROW">
        <UNIT_OF_MEASURE><xsl:apply-templates/></UNIT_OF_MEASURE>            
    </xsl:template>
    <xsl:template priority="2" match="ROW">
        <COMMODITY><xsl:apply-templates/></COMMODITY>            
    </xsl:template>
    <xsl:template priority="2" match="PRICES/PRICES_ROW">
        <PRICE><xsl:apply-templates/></PRICE>            
    </xsl:template>       
    <xsl:template priority="2" match="MINIMAL_PRICES/MINIMAL_PRICES_ROW">
        <MINIMAL_PRICE><xsl:apply-templates/></MINIMAL_PRICE>            
    </xsl:template>       
    <xsl:template priority="2" match="GROUPS/GROUPS_ROW">
        <GROUP><xsl:apply-templates/></GROUP>            
    </xsl:template>                               
</xsl:stylesheet>');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt)
             VALUES (
                        jg_sqre_seq.NEXTVAL,
                        'CONTRACTORS',
                        'SELECT konr.symbol customer_number,
       konr_payer.symbol payer_number,       
       konr.nazwa name,
       konr.skrot short_name,
       konr.nip nip,
       konr.blokada_sprz order_blockade,
       konr.aktualny active,
       konr.platnik is_payer,
       konr.odbiorca is_reciever,
       konr.nr_tel phone,
       konr.nr_faksu fax,
       konr.mail email,
       konr.dni_do_zaplaty day_topay,
       LG_KNR_LIKR_SQL.Aktualny_Limit_Konr_Kwota (KONR.ID, Pa_Sesja.Dzisiaj) credit_limit,
       ''T'' representative,
       konr.nr_umowy_ind default_financing_method,
       CURSOR (
           SELECT ulica street,
                  nr_domu house_number,
                  nr_lokalu flat_number,
                  miejscowosc city,
                  kod_pocztowy post_code
             FROM lg_kntrh_adresy_konr_vw
            WHERE     konr_id = konr.id
                  AND typ_adresu = ''GEOGRAFICZNY''
                  AND rola_adresu = ''SIEDZIBY'')
           legal_addresses,
       CURSOR (
           SELECT ulica street,
                  nr_domu house_number,
                  nr_lokalu flat_number,
                  miejscowosc city,
                  kod_pocztowy post_code
             FROM lg_kntrh_adresy_konr_vw
            WHERE     konr_id = konr.id
                  AND typ_adresu = ''GEOGRAFICZNY''
                  AND rola_adresu = ''DOSTAWY'')
           delivery_addresses
  FROM ap_kontrahenci konr, ap_kontrahenci konr_payer
 WHERE konr_payer.id(+) = konr.platnik_id AND konr.id IN (:p_id)',
                        '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
        <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()" />
        </xsl:copy>
    </xsl:template>
    <xsl:template priority="2" match="ROW">
        <CONTRACTOR><xsl:apply-templates/></CONTRACTOR>            
    </xsl:template>
    <xsl:template priority="2" match="LEGAL_ADDRESSES/LEGAL_ADDRESSES_ROW">
        <LEGAL_ADDRESS><xsl:apply-templates/></LEGAL_ADDRESS>            
    </xsl:template>
    <xsl:template priority="2" match="DELIVERY_ADDRESSES/DELIVERY_ADDRESSES_ROW">
        <DELIVERY_ADDRESS><xsl:apply-templates/></DELIVERY_ADDRESS>            
    </xsl:template>                         
</xsl:stylesheet>');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt)
             VALUES (
                        jg_sqre_seq.NEXTVAL,
                        'INVOICES',
                        'SELECT header.symbol invoice_symbol,
       header.doc_type,
       header.doc_date invoice_date,
       header.sale_date sale_date,
       header.payment_date payment_date,
       header.currency currency,
       header.net_value net_value,
       header.gross_value,
       lg_dosp_sql.kwota_zaplat_na_dok (id) amount_paid,
       CASE
           WHEN header.gross_value <= lg_dosp_sql.kwota_zaplat_na_dok (id)
           THEN
               ''T''
           ELSE
               ''N''
       END
           is_paid,
       header.payer_symbol payer_symbol,
       header.payer_name,   
       header.payer_nip,
       header.payer_city,
       header.payer_postal_code payer_post_code,
       header.payer_street,
       header.payer_building,
       header.payer_apartment,       
       header.receiver_symbol,
       header.receiver_name,       
       header.delivery_type,
       CURSOR (SELECT ordinal ordinal,
                      item_symbol item_symbol,
                      item_name item_name,
                      unit unit_of_measure_code,
                      quantity quantity,
                      net_price net_price,
                      vat_percent vat_rate,
                      net_value,
                      vat_value,
                      gross_value
                 FROM lg_sal_invoices_it
                WHERE line_type IN (''N'', ''P'') AND document_id = header.id)
           lines
  FROM lg_sal_invoices header
 WHERE     header.approved = ''T''
       AND doc_type IN (''FS'', ''KS'')
       AND header.id IN ( :p_id)',
                        '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
        <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()" />
        </xsl:copy>
    </xsl:template>
    <xsl:template priority="2" match="ROW">
        <INVOICE><xsl:apply-templates/></INVOICE>            
    </xsl:template>
    <xsl:template priority="2" match="LINES/LINES_ROW">
        <LINE><xsl:apply-templates/></LINE>            
    </xsl:template>                
</xsl:stylesheet>');
END;
/