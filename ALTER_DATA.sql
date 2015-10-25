DECLARE
    v_order_clob   CLOB;
BEGIN
    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
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
</xsl:stylesheet>',
                        'IN/invoices_payments',
                        'N',
                        'OUT');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
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
</xsl:stylesheet>',
                        'IN/commodities',
                        'T',
                        'OUT');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
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
       lg_knr_likr_sql.aktualny_limit_konr_kwota (konr.id, pa_sesja.dzisiaj)
           credit_limit,
       (SELECT MAX (osol.kod)
          FROM lg_osoby_log osol,
               (    SELECT *
                      FROM lg_grupy_kontrahentow
                START WITH id = 63
                CONNECT BY PRIOR id = grkn_id) grko,
               lg_kontrahenci_grup kngr
         WHERE     osol.atrybut_t01 = grko.nazwa
               AND grko.id = kngr.grkn_id
               AND osol.aktualna = ''T''
               AND kngr.konr_id = konr.id)
           representative,
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
 WHERE konr_payer.id(+) = konr.platnik_id AND konr.id IN ( :p_id)',
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
</xsl:stylesheet>',
                        'IN/contractors',
                        'T',
                        'OUT');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
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
</xsl:stylesheet>',
                        'IN/invoices',
                        'N',
                        'OUT');

    v_order_clob :=
        'SELECT header.*,
                         wzrc.pricing_type pricing_type,
                         pa_firm_sql.kod (wzrc.firm_id) company_code,
                         wzrc.name wzorzec,
                         wzrc.place_of_issue place_of_issue,
                         NVL (wzrc.base_currency, wzrc.currency) currency,
                         pusp.kod pusp_kod,
                         CURSOR (
                             SELECT konr.symbol,
                                    konr.nazwa,
                                    konr.skrot,
                                    konr.nip,
                                    adge.miejscowosc,
                                    adge.kod_pocztowy,
                                    adge.ulica,
                                    adge.nr_domu,
                                    adge.nr_lokalu,
                                    adge.poczta
                               FROM ap_kontrahenci konr, pa_adr_adresy_geograficzne adge
                              WHERE     adge.id = lg_konr_adresy.adge_id_siedziby (konr.id)
                                    AND konr.id = wzrc.issuer_id)
                             sprzedawca,
                         CURSOR (
                             SELECT konr.symbol,
                                    konr.nazwa,
                                    konr.skrot,
                                    konr.nip,
                                    adge.miejscowosc,
                                    adge.kod_pocztowy,
                                    adge.ulica,
                                    adge.nr_domu,
                                    adge.nr_lokalu,
                                    adge.poczta
                               FROM ap_kontrahenci konr, pa_adr_adresy_geograficzne adge
                              WHERE     adge.id = lg_konr_adresy.adge_id_siedziby (konr.id)
                                    AND konr.symbol = header.customer_symbol)
                             platnik,
                         CURSOR (
                             SELECT konr.symbol,
                                    konr.nazwa,
                                    konr.skrot,
                                    konr.nip,
                                    adge.miejscowosc,
                                    adge.kod_pocztowy,
                                    adge.ulica,
                                    adge.nr_domu,
                                    adge.nr_lokalu,
                                    adge.poczta
                               FROM ap_kontrahenci konr, pa_adr_adresy_geograficzne adge
                              WHERE     adge.id = lg_konr_adresy.adge_id_siedziby (konr.id)
                                    AND konr.symbol = header.supplier_symbol)
                             odbiorca,
                         CURSOR (
                                          SELECT item_xml.*,
                                                 inma.nazwa commodity_name,
                                                 inma.jdmr_nazwa_pdst_sp jdmr_nazwa,
                                                 api_rk_stva.kod (inma.stva_id) kod_stawki_vat,
                                                 NVL (wzrc.base_currency, wzrc.currency) currency
                                            FROM jg_input_log log1,
                                                 ap_indeksy_materialowe inma,
                                                 XMLTABLE (
                                                     ''//Order/Items/Item''
                                                     PASSING xmltype (log1.xml)
                                                     COLUMNS ordinal_number     VARCHAR2 (30)
                                                                                    PATH ''/Item/OrdinalNumber'',
                                                             note               VARCHAR2 (30)
                                                                                    PATH ''/Item/Comment'',
                                                             commodity_id       VARCHAR2 (30)
                                                                                    PATH ''/Item/CommodityID'',
                                                             quantity_ordered   VARCHAR2 (30)
                                                                                    PATH ''/Item/QuantityOrdered'',
                                                             discount           VARCHAR2 (30)
                                                                                    PATH ''/Item/Discount'',
                                                             promotion_code     VARCHAR2 (30)
                                                                                    PATH ''/Item/PromotionCode'',
                                                             promotion_name     VARCHAR2 (30)
                                                                                    PATH ''/Item/PromotionName'',
                                                             net_price          VARCHAR2 (30)
                                                                                    PATH ''/Item/NetPrice'',
                                                             commodity_ean      VARCHAR2 (30)
                                                                                    PATH ''/Item/CommodityEAN'') item_xml
                                           WHERE     log1.id = LOG.id
                                                 AND inma.indeks = item_xml.commodity_id)
                             items
                    FROM jg_input_log LOG,
                         lg_documents_templates wzrc,
                         lg_punkty_sprzedazy pusp,
                         XMLTABLE (
                             ''//Order''
                             PASSING xmltype (LOG.xml)
                             COLUMNS id VARCHAR2 (30) PATH ''/Order/ID'',
                                     customer_symbol VARCHAR2 (30) PATH ''/Order/CustomerID'',
                                     supplier_symbol VARCHAR2 (30) PATH ''/Order/SupplierID'',
                                     realization_date DATE PATH ''/Order/RealizationDate'',
                                     sales_representative_id VARCHAR2 (30)
                                         PATH ''/Order/SalesRepresentativeID'',
                                     payment_method_id VARCHAR2 (30)
                                         PATH ''/Order/PaymentMethodID'',
                                     delivery_method_id VARCHAR2 (30)
                                         PATH ''/Order/DeliveryMethodID'',
                                     discount VARCHAR2 (30) PATH ''/Order/Discount'',
                                     note VARCHAR2 (30) PATH ''/Order/Comment'',
                                     net_value VARCHAR2 (30) PATH ''/Order/NetValue'') header
                   WHERE     pusp.id = wzrc.pusp_id
                         AND wzrc.id = :p_wzrc_id
                         AND LOG.id = :p_operation_id';

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
         VALUES (jg_sqre_seq.NEXTVAL,
                 'ORDER',
                 v_order_clob,
                 NULL,
                 'OUT/orders',
                 'T',
                 'IN');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
             VALUES (
                        jg_sqre_seq.NEXTVAL,
                        'RESERVATIONS',
                        'SELECT zare.dest_symbol            order_id,
                             zare.data_realizacji        realization_date,
                             inma.indeks                 commoditiy_id,
                             zare.ilosc                  quantity_ordered,
                             reze.ilosc_zarezerwowana    quantity_reserved
                        FROM lg_rzm_rezerwacje reze,
                             lg_rzm_zadania_rezerwacji zare,
                             ap_indeksy_materialowe inma
                       WHERE     reze.zare_id = zare.id
                             AND zare.inma_id = inma.id
                             AND reze.id IN (:p_id)',
                        NULL,
                        NULL,
                        'N',
                        'OUT');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
             VALUES (
                        jg_sqre_seq.NEXTVAL,
                        'SETS_COMPONENTS',
                        'SELECT inma_kpl.indeks set_id,
                             inma_kpl.nazwa set_name,
                             Lg_Stm_Sgpu_Sql.Stan_Goracy(inma_kpl.id, inma_kpl.jdmr_nazwa, null) available_stock,
                             inma_kpl.atrybut_n05 price_before_discount,
                             inma_kpl.atrybut_n06 price_after_discount,
                             inma_kpl.atrybut_d01 valid_date,
                             CURSOR (SELECT inma_skpl.indeks commodity_id,
                                            inma_skpl.nazwa commodity_name,
                                            kpsk1.ilosc quantity,
                                            kpsk1.premiowy bonus,
                                            DECODE(kpsk1.dynamiczny, ''T'', ''DYNAMIC'', ''STATIC'') set_type,
                                            DECODE(inma_skpl.atrybut_t03, ''T'', ''N'', ''Y'') contract_payment
                                       FROM lg_kpl_skladniki_kompletu kpsk1,
                                            ap_indeksy_materialowe inma_skpl
                                      WHERE     kpsk1.skl_inma_id = inma_skpl.id
                                            AND kpsk1.kpl_inma_id = kpsk.kpl_inma_id) components
                        FROM lg_kpl_skladniki_kompletu kpsk,
                             ap_indeksy_materialowe inma_kpl
                       WHERE     kpsk.kpl_inma_id = inma_kpl.id
                             AND ROWNUM = 1
                             AND kpsk.kpl_inma_id IN (:p_id)',
                        '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
        <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()" />
        </xsl:copy>
    </xsl:template>
    <xsl:template priority="2" match="ROW">
        <SET_COMPONENTS><xsl:apply-templates/></SET_COMPONENTS>            
    </xsl:template>
    <xsl:template priority="2" match="COMPONENTS/COMPONENTS_ROW">
        <COMPONENT><xsl:apply-templates/></COMPONENT>            
    </xsl:template>                
</xsl:stylesheet>',
                        'IN/components',
                        'T',
                        'OUT');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
         VALUES (jg_sqre_seq.NEXTVAL,
                 'SALES_REPRESENTATIVES',
                 'SELECT osol.kod code,
       osol.first_name,
       osol.surname,
       osol.atrybut_t01 region,
       osol.aktualna up_to_date,
       CURSOR (
           SELECT konr.symbol  customer_number
             FROM ap_kontrahenci konr,
                  lg_kontrahenci_grup kngr,
                  (    SELECT *
                         FROM lg_grupy_kontrahentow
                   START WITH id = 63
                   CONNECT BY PRIOR id = grkn_id) grkn
            WHERE     kngr.konr_id = konr.id
                  AND kngr.grkn_id = grkn.id
                  AND grkn.nazwa = osol.atrybut_t01)
           contractors
  FROM lg_osoby_log osol
 WHERE atrybut_t01 IS NOT NULL AND osol.id IN ( :p_id)',
                 '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
        <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()" />
        </xsl:copy>
    </xsl:template>
    <xsl:template priority="2" match="ROW">
        <SALES_REPRESENTATIVE><xsl:apply-templates/></SALES_REPRESENTATIVE>            
    </xsl:template>
    <xsl:template priority="2" match="CONTRACTORS/CONTRACTORS_ROW">
        <CONTRACTOR><xsl:apply-templates/></CONTRACTOR>            
    </xsl:template>                
</xsl:stylesheet>',
                 'IN/sales_representatives',
                 'T',
                 'OUT');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
             VALUES (
                        jg_sqre_seq.NEXTVAL,
                        'NEW_CONTRACTORS',
                        'SELECT osol.kod id,
       osby.code name,
       osol.aktualna active,
       osol.first_name username,
       osol.surname usersurname,
       CURSOR (
           SELECT konr.symbol customerid
             FROM lg_osoby_log osol1,
                  (    SELECT *
                         FROM lg_grupy_kontrahentow
                   START WITH id = 63
                   CONNECT BY PRIOR id = grkn_id) grko,
                  lg_kontrahenci_grup kngr,
                  ap_kontrahenci konr
            WHERE     osol1.atrybut_t01 = grko.nazwa
                  AND grko.id = kngr.grkn_id
                  AND osol1.aktualna = ''T''
                  AND kngr.konr_id = konr.id
                  AND osol1.id = osol.id)
           customers
  FROM lg_osoby_log osol, pa_osoby osby
 WHERE     osol.atrybut_t01 IS NOT NULL
       AND osol.osby_id = osby.id
       AND osol.id IN ( :p_id)',
                        '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
        <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()" />
        </xsl:copy>
    </xsl:template>
    <xsl:template priority="2" match="ROW">
        <SALES_REPRESENTATIVE><xsl:apply-templates/></SALES_REPRESENTATIVE>            
    </xsl:template>
    <xsl:template priority="2" match="CUSTOMERS/CUSTOMERS_ROW">
        <CUSTOMER><xsl:apply-templates/></CUSTOMER>            
    </xsl:template>                
</xsl:stylesheet>',
                        'OUT/new_customer',
                        'T',
                        'IN');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
         VALUES (jg_sqre_seq.NEXTVAL,
                 'DELIVERY_METHODS',
                 'SELECT kod delivery_method_code,
       opis description,
       aktualna up_to_date
  FROM ap_sposoby_dostaw spdo
 WHERE spdo.id IN ( :p_id)',
                 '<xsl:stylesheet version="1.0"
xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml" version="1.5" indent="yes"
  omit-xml-declaration="no" />
  <xsl:template match="@*|node()">
    <xsl:copy>
      <xsl:apply-templates select="@*|node()" />
    </xsl:copy>
  </xsl:template>
  <xsl:template priority="2" match="ROW">
    <DELIVERY_METHOD>
      <xsl:apply-templates />
    </DELIVERY_METHOD>
  </xsl:template>
</xsl:stylesheet>',
                 'IN/delivery_methods',
                 'T',
                 'OUT');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
         VALUES (jg_sqre_seq.NEXTVAL,
                 'PAYMENTS_METHODS',
                 'SELECT foza.kod payment_method_code,
       foza.opis description,
       odroczenie_platnosci deferment_of_payment,
       (SELECT rv_meaning
          FROM cg_ref_codes
         WHERE rv_domain = ''FORMY_ZAPLATY'' AND rv_low_value = foza.typ)
           payment_type,
       aktualna up_to_date
  FROM ap_formy_zaplaty foza
 WHERE foza.id IN ( :p_id)',
                 '<xsl:stylesheet version="1.0"
xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml" version="1.5" indent="yes"
  omit-xml-declaration="no" />
  <xsl:template match="@*|node()">
    <xsl:copy>
      <xsl:apply-templates select="@*|node()" />
    </xsl:copy>
  </xsl:template>
  <xsl:template priority="2" match="ROW">
    <PAYMENT_METHOD>
      <xsl:apply-templates />
    </PAYMENT_METHOD>
  </xsl:template>
</xsl:stylesheet>',
                 'IN/payments_methods',
                 'T',
                 'OUT');
				 
    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
         VALUES (jg_sqre_seq.NEXTVAL,
                 'ORDERS_PATTERNS',
                 'SELECT wzrc.pattern pattern_code, wzrc.name pattern_name, wzrc.up_to_date
  FROM lg_documents_templates wzrc
 WHERE document_type = ''ZS'' AND wzrc.id IN ( :p_id)',
                 '<xsl:stylesheet version="1.0"
xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml" version="1.5" indent="yes"
  omit-xml-declaration="no" />
  <xsl:template match="@*|node()">
    <xsl:copy>
      <xsl:apply-templates select="@*|node()" />
    </xsl:copy>
  </xsl:template>
  <xsl:template priority="2" match="ROW">
    <ORDER_PATTERN>
      <xsl:apply-templates />
    </ORDER_PATTERN>
  </xsl:template>
</xsl:stylesheet>',
                 'IN/orders_patterns',
                 'T',
                 'OUT');				 
END;
/
