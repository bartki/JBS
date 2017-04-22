DECLARE
    v_order_clob   CLOB;
    v_xslt         CLOB;
BEGIN

    INSERT INTO jg_sql_repository (id,
                                   object_type,                                   
                                   file_location,
                                   up_to_date,
                                   direction)
        VALUES (
                   jg_sqre_seq.NEXTVAL,
                   'CASH_RECEIPTS',                   
                   'OUT/new_kp',
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
                   'INVOICES_PAYMENTS',
                   '
SELECT rndo.symbol_dokumentu invoice_number,
         rndo.data_dokumentu invoice_date,
         rndo.termin_platnosci due_date,
         rndo.forma_platnosci payment_form,
         konr.symbol payer_symbol,
         konr.nazwa payer_name,
         jg_output_sync.format_number (rndo.wartosc_dok_z_kor_wwb, 2) total,
         jg_output_sync.format_number (rndo.poz_do_zaplaty_dok_z_kor_wwb, 2)
             amount_left,
         CURSOR (
             SELECT rnwp.symbol_dokumentu payment_doc_number,
                    rnwp.data_dokumentu payment_date,
                    jg_output_sync.format_number (rnwp.zaplata_wwb, 2)
                        amount_paid
               FROM rk_rozr_nal_dok_plat_rk_vw rnwp
              WHERE     rnwp.rndo_id = rndo.rndo_id
                    AND rnwp.zaplata_wwb IS NOT NULL
                    AND rnwp.typ = ''P'')
             payments_details
    FROM rk_rozr_nal_dokumenty_vw rndo, ap_kontrahenci konr
   WHERE     konr.id = rndo.konr_id
         AND rndo.rnwp_rnwp_id IS NULL
         AND rndo.typ IN (''FAK'', ''KOR'')
         and rndo.poz_do_zaplaty_dok_z_kor_wwb > 0
         AND rndo.rndo_id IN ( :p_id)
GROUP BY rndo.symbol_dokumentu,
         rndo.termin_platnosci,
         rndo.forma_platnosci,
         konr.id,
         konr.symbol,
         konr.nazwa,
         rndo.wartosc_dok_z_kor_wwb,
         rndo.poz_do_zaplaty_dok_z_kor_wwb,
         rndo.rndo_id,
          rndo.data_dokumentu',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <INVOICE_PAYMENTS><xsl:apply-templates/></INVOICE_PAYMENTS>
                     </xsl:template>
                     <xsl:template priority="2" match="PAYMENTS_DETAILS/PAYMENTS_DETAILS_ROW">
                        <PAYMENT_DETAIL><xsl:apply-templates/></PAYMENT_DETAIL>
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/invoices_payments',
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
                   'COMMODITIES',
                   'SELECT inma.indeks item_index,
       inma.nazwa name,
       ec ec,
       inma.jdmr_nazwa base_unit_of_measure_code,
       BIN_TO_NUM (DECODE (atrybut_c01, ''T'', 1, 0),
                   DECODE (ec, ''T'', 1, 0),
                   DECODE (w_ofercie, ''T'', 1, 0),
                   DECODE (mozliwe_sprzedawanie, ''T'', 1, 0)) AVAILABILITY,
       (SELECT MAX (kod_kreskowy) ean
          FROM lg_przeliczniki_jednostek prje
         WHERE     prje.kod_kreskowy IS NOT NULL
               AND prje.inma_id = inma.id
               AND prje.jdmr_nazwa = inma.jdmr_nazwa)
           base_ean_code,
       (SELECT rv_meaning
          FROM cg_ref_codes
         WHERE rv_domain = ''LG_CECHY_INMA'' AND rv_low_value = inma.cecha)
           TYPE,
       (SELECT stva.stopa
          FROM rk_stawki_vat stva
         WHERE stva.id = inma.stva_id)
           vat_rate,
       NVL ( (SELECT zapas_min
                FROM ap_inma_maga_zapasy inmz
               WHERE inmz.inma_id = inma.id AND inmz.maga_id = 500),
            0)
           min_stock,
       CURSOR (SELECT jdmr_nazwa unit_of_measure_code, kod_kreskowy ean_code
                 FROM lg_przeliczniki_jednostek prje
                WHERE prje.inma_id = inma.id)
           units_of_measure,
       CURSOR (
           SELECT walu.kod currency,
                  jg_output_sync.format_number (cezb.cena, 4) net_price,
                  jg_output_sync.format_number (cezb.cena_brutto, 4)
                      gross_price,
                  cezb.jdmr_nazwa unit_of_measure_code,
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
           SELECT jg_output_sync.format_number (wace.price_min_net, 4)
                      net_price,
                  jg_output_sync.format_number (wace.price_min_gross, 4)
                      gross_price,
                  wace.jdmr_nazwa unit_of_measure_code
             FROM lg_wah_warunki_cen wace
            WHERE     wace.price_min_net IS NOT NULL
                  AND wace.price_min_gross IS NOT NULL
                  AND wace.data_od <= SYSDATE
                  AND (wace.data_do >= SYSDATE OR wace.data_do IS NULL)
                  AND wace.inma_id = inma.id)
           minimal_prices,
       CURSOR (
           SELECT gras.grupa_asortymentowa group_name,
                  gras.kod group_code,
                  grin.podstawowa is_primary
             FROM ap_grupy_indeksow grin, ap_grupy_asortymentowe gras
            WHERE     gras.id = grin.gras_id
                  AND grin.inma_id = inma.id
                  AND gras.id IN (SELECT gras.id
                                    FROM ap_grupy_asortymentowe gras
                                  CONNECT BY PRIOR gras.id = gras.gras_id_nad
                                  START WITH gras.kod = ''GRAS 2013''))
           groups
  FROM ap_indeksy_materialowe inma
 WHERE inma.aktualny = ''T'' AND inma.id IN (:p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
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
                         NVL(konr.aktualny, ''N'') active,
                         konr.platnik is_payer,
                         konr.odbiorca is_reciever,
                         konr.potential potential,
                         konr.nr_tel phone,
                         konr.nr_faksu fax,
                         konr.mail email,
                         --konr.dni_do_zaplaty day_topay,
                         jg_output_sync.format_number(lg_knr_likr_sql.aktualny_limit_konr_kwota(konr.id, pa_sesja.dzisiaj), 2) credit_limit,
                         (SELECT grupa
                            FROM ap_grupy_odbiorcow grod
                           WHERE grod.id = konr.grod_id) reciever_group,
                         (SELECT MAX(osol.kod)
                            FROM lg_osoby_log osol,
                         (SELECT *
                            FROM lg_grupy_kontrahentow
                           START WITH id = 63
                      CONNECT BY PRIOR id = grkn_id) grko,
                                 lg_kontrahenci_grup kngr
                           WHERE     osol.atrybut_t01 = grko.nazwa
                                 AND grko.id = kngr.grkn_id
                                 AND osol.aktualna = ''T''
                                 AND kngr.konr_id = konr.id) representative,
                                     konr.foza_kod default_financing_method,
                          CURSOR (SELECT ulica        street,
                                         nr_domu      house_number,
                                         nr_lokalu    flat_number,
                                         miejscowosc  city,
                                         kod_pocztowy post_code
                                    FROM lg_kntrh_adresy_konr_vw
                                   WHERE     konr_id = konr.id
                                         AND typ_adresu = ''GEOGRAFICZNY''
                                         AND rola_adresu = ''SIEDZIBY'') legal_addresses,
                          CURSOR (SELECT ulica        street,
                                         nr_domu      house_number,
                                         nr_lokalu    flat_number,
                                         miejscowosc  city,
                                         kod_pocztowy post_code
                                    FROM lg_kntrh_adresy_konr_vw
                                   WHERE     konr_id = konr.id
                                         AND typ_adresu = ''GEOGRAFICZNY''
                                         AND rola_adresu = ''DOSTAWY'') delivery_addresses
                    FROM ap_kontrahenci konr, ap_kontrahenci konr_payer
                   WHERE     konr_payer.id(+) = konr.platnik_id
                         AND konr.id IN (:p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                    <xsl:output method="xml" version="1.0" indent="yes" omit-xml-declaration="no" />
                    <xsl:strip-space elements="*"/>
                    <xsl:template match="node()|@*">
                       <xsl:copy>
                          <xsl:apply-templates select="node()|@*"/>
                       </xsl:copy>
                    </xsl:template>
                    <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
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
       (SELECT symbol
          FROM lg_sal_orders sord
         WHERE sord.id = header.source_order_id)
           order_symbol,
       header.doc_type,
       header.doc_date invoice_date,
       header.sale_date sale_date,
       header.payment_date payment_date,
       header.currency currency,
       jg_output_sync.format_number (header.net_value, 2) net_value,
       jg_output_sync.format_number (header.gross_value, 2) gross_value,
       jg_output_sync.format_number (lg_dosp_sql.kwota_zaplat_na_dok (id), 2)
           amount_paid,
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
       CURSOR (
           SELECT ordinal ordinal,
                  item_symbol item_symbol,
                  item_name item_name,
                  unit unit_of_measure_code,
                  jg_output_sync.format_number (quantity, 100) quantity,
                  jg_output_sync.format_number (net_price, 2) net_price,
                  jg_output_sync.format_number (vat_percent, 2) vat_rate,
                  jg_output_sync.format_number (net_value, 2) net_value,
                  jg_output_sync.format_number (vat_value, 2) vat_value,
                  jg_output_sync.format_number (gross_value, 2) gross_value
             FROM lg_sal_invoices_it
            WHERE line_type IN (''N'', ''P'') AND document_id = header.id)
           lines
  FROM lg_sal_invoices header
 WHERE     header.approved = ''T''
       AND doc_type IN (''FS'', ''KS'')
       AND header.id IN (:p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <INVOICE><xsl:apply-templates/></INVOICE>
                     </xsl:template>
                     <xsl:template priority="2" match="LINES/LINES_ROW">
                        <LINE><xsl:apply-templates/></LINE>
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/invoices',
                   'T',
                   'OUT');

    v_order_clob :=
        'SELECT header.*,
                sord.guid,
                            TRUNC(TO_DATE(header.order_issue_date_bc, ''YYYY-MM-DD"T"HH24:MI:SS'')) order_issue_date,
                            TRUNC(TO_DATE(header.requested_delivery_date_bc, ''YYYY-MM-DD"T"HH24:MI:SS'')) requested_delivery_date,
                            wzrc.document_type document_type,
                            wzrc.pricing_type pricing_type,
                            pa_firm_sql.kod (wzrc.firm_id) company_code,
                            wzrc.place_of_issue place_of_issue,
                            NVL (wzrc.base_currency, wzrc.currency) currency,
                            NVL(header.payment_date, wzrc.payment_days) payment_days,
                            pusp.kod pusp_kod,
                            NVL(header.net_value * (header.order_discount/ 100), 0) order_discount_value,
                            CURSOR ( SELECT konr.symbol,
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
                                            AND konr.id = wzrc.issuer_id) sprzedawca,
                            CURSOR ( SELECT konr.symbol,
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
                                            AND konr.symbol = header.seller_buyer_id) platnik,
                            CURSOR ( SELECT konr.symbol,
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
                                            AND konr.symbol = header.receiver_id ) odbiorca,
                            CURSOR ( SELECT item_xml.*,
                                            sori.guid,
                                            (item_xml.unit_price_base - item_xml.unit_price_value) discount_value,
                                            inma.nazwa commodity_name,
                                            inma.jdmr_nazwa_pdst_sp jdmr_nazwa,
                                            api_rk_stva.kod (inma.stva_id) inma_stva_code,
                                            NVL (wzrc.base_currency, wzrc.currency) currency
                                       FROM jg_input_log log1,
                                            ap_indeksy_materialowe inma,
                                            XMLTABLE ( ''//Order/OrderDetail/Item''
                                                       PASSING xmltype (log1.xml)
                                                       COLUMNS item_num               VARCHAR2 (30) PATH ''/Item/ItemNum'',
                                                               seller_item_id         VARCHAR2 (30) PATH ''/Item/SellerItemID'',
                                                               name                   VARCHAR2 (70) PATH ''/Item/Name'',                                                               
                                                               unit_of_measure        VARCHAR2 (30) PATH ''/Item/UnitOfMeasure'',                                                               
                                                               quantity_value         VARCHAR2 (30) PATH ''/Item/QuantityValue'',
                                                               tax_percent            VARCHAR2 (30) PATH ''/Item/TaxPercent'',
                                                               unit_price_value       VARCHAR2 (30) PATH ''/Item/UnitPriceValue'',
                                                               unit_price_base        VARCHAR2 (30) PATH ''/Item/UnitPriceBase'',
                                                               unit_discount_value    VARCHAR2 (30) PATH ''/Item/UnitDiscountValue'',
                                                               unit_discount          VARCHAR2 (30) PATH ''/Item/UnitDiscount'',
                                                               description                  VARCHAR2(500) PATH ''/Item/Description'',
                                                               promotion_code         VARCHAR2 (500) PATH ''/Item/PromotionCode'',
                                                               promotion_name         VARCHAR2 (500) PATH ''/Item/PromotionName'') item_xml,
                                            lg_sal_orders_it sori                                                           
                                      WHERE     log1.id = LOG.id
                                            AND inma.indeks = item_xml.seller_item_id
                                            AND (    sori.document_id (+) = sord.id
                                                 AND sori.item_symbol (+) = item_xml.seller_item_id)
                                                 AND sori.ordinal (+) = item_xml.item_num) items
                       FROM jg_input_log LOG,
                            lg_documents_templates wzrc,
                            lg_punkty_sprzedazy pusp,
                            XMLTABLE ( ''//Order''
                                       PASSING xmltype (LOG.xml)
                                       COLUMNS order_number               VARCHAR2 (30)      PATH ''/Order/OrderHeader/OrderNumber'',
                                              order_pattern                         VARCHAR2 (100)     PATH ''/Order/OrderHeader/OrderPattern'',
                                              order_type                              VARCHAR2 (1)       PATH ''/Order/OrderHeader/OrderType'',
                                              order_issue_date_bc              VARCHAR2 (30)      PATH ''/Order/OrderHeader/OrderIssueDate'',
                                              requested_delivery_date_bc  VARCHAR2 (30)      PATH ''/Order/OrderHeader/RequestedDeliveryDate'',
                                              note                                         VARCHAR2 (100)     PATH ''/Order/OrderHeader/Comment'',
                                              payment_date                         VARCHAR2(30)     PATH ''/Order/OrderHeader/PaymentDate'',
                                              order_discount                        VARCHAR2 (1)       PATH ''/Order/OrderHeader/OrderDiscount'',                                               
                                              payment_method_code          VARCHAR2 (6)       PATH ''/Order/OrderHeader/PaymentMethod/Code'',
                                              transportation_code               VARCHAR2 (3)       PATH ''/Order/OrderHeader/Transportation/Code'',                                               
                                              seller_buyer_id                       VARCHAR2 (30)      PATH ''/Order/OrderParty/BuyerParty/SellerBuyerID'',
                                              seller_contact_tel                   VARCHAR2 (30)      PATH ''/Order/OrderParty/BuyerParty/Contact/Tel'',
                                              receiver_id                              VARCHAR2(30)       PATH ''/Order/OrderParty/ShipToParty/CustomerNumber'',
                                              sr_party_description              VARCHAR2 (30)      PATH ''/Order/OrderParty/SRParty/Description'',
                                              net_value                               VARCHAR2(30)        PATH ''/Order/OrderSummary/TotalNetAmount'',
                                              gross_value                            VARCHAR2(30)        PATH ''/Order/OrderSummary/TotalGrossAmount'' ) header,
                            lg_sal_orders sord
                      WHERE     pusp.id = wzrc.pusp_id
                            AND wzrc.pattern = header.order_pattern
                            AND (    sord.doc_symbol_rcv(+) = header.order_number
                                 AND sord.payer_symbol(+) = header.seller_buyer_id)
                            AND LOG.id = :p_operation_id';

    v_xslt :=
        '<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml" encoding="windows-1250" indent="yes"/>
  <xsl:template match="/">
    <LG_ZASP_T>
      <xsl:for-each select="ORDER">
        <xsl:for-each select="ORDER_NUMBER">
          <SYMBOL_ODBIORCY>
            <xsl:value-of select="."/>
          </SYMBOL_ODBIORCY>
        </xsl:for-each>
        <xsl:for-each select="GUID">
          <GUID_DOKUMENTU>
            <xsl:value-of select="."/>
          </GUID_DOKUMENTU>
        </xsl:for-each>
        <xsl:for-each select="ORDER_PATTERN">
          <WZORZEC>
            <xsl:value-of select="."/>
          </WZORZEC>
        </xsl:for-each>
        <xsl:for-each select="DOCUMENT_TYPE">
          <TYP_ZAMOWIENIA>
            <xsl:value-of select="."/>
          </TYP_ZAMOWIENIA>
        </xsl:for-each>
        <xsl:for-each select="ORDER_ISSUE_DATE">
          <DATA_WYSTAWIENIA>
            <xsl:value-of select="."/>
          </DATA_WYSTAWIENIA>
        </xsl:for-each>
        <xsl:for-each select="REQUESTED_DELIVERY_DATE">
          <DATA_REALIZACJI>
            <xsl:value-of select="."/>
          </DATA_REALIZACJI>
        </xsl:for-each>
        <xsl:for-each select="PLACE_OF_ISSUE">
          <MIEJSCE_WYSTAWIENIA>
            <xsl:value-of select="."/>
          </MIEJSCE_WYSTAWIENIA>
        </xsl:for-each>
        <xsl:for-each select="COMPANY_CODE">
          <KOD_FIRMY>
            <xsl:value-of select="."/>
          </KOD_FIRMY>
        </xsl:for-each>
        <xsl:for-each select="NOTE">
          <UWAGI>
            <xsl:value-of select="."/>
          </UWAGI>
        </xsl:for-each>
        <xsl:for-each select="CURRENCY">
          <KOD_WALUTY_CENNIKA>
            <xsl:value-of select="."/>
          </KOD_WALUTY_CENNIKA>
        </xsl:for-each>
        <xsl:for-each select="PAYMENT_METHOD_CODE">
          <KOD_FORMY_ZAPLATY>
            <xsl:value-of select="."/>
          </KOD_FORMY_ZAPLATY>
        </xsl:for-each>
        <xsl:for-each select="TRANSPORTATION_CODE">
          <KOD_SPOSOBU_DOSTAWY>
            <xsl:value-of select="."/>
          </KOD_SPOSOBU_DOSTAWY>
        </xsl:for-each>
        <xsl:for-each select="PRICING_TYPE">
          <WG_JAKICH_CEN>
            <xsl:value-of select="."/>
          </WG_JAKICH_CEN>
        </xsl:for-each>
        <xsl:for-each select="ORDER_DISCOUNT_VALUE">
          <OPUST_GLOB_KWOTA>
            <xsl:value-of select="."/>
          </OPUST_GLOB_KWOTA>
        </xsl:for-each>
        <xsl:for-each select="ORDER_DISCOUNT">
          <OPUST_GLOB_PROC_OD_WART_BEZ_UP>
            <xsl:value-of select="."/>
          </OPUST_GLOB_PROC_OD_WART_BEZ_UP>
        </xsl:for-each>
        <xsl:for-each select="PAYMENT_DAYS">
          <ILOSC_DNI_DO_ZAPLATY>
            <xsl:value-of select="."/>
          </ILOSC_DNI_DO_ZAPLATY>
        </xsl:for-each>
        <xsl:for-each select="PUSP_KOD">
          <KOD_PUNKTU_SPRZEDAZY>
            <xsl:value-of select="."/>
          </KOD_PUNKTU_SPRZEDAZY>
        </xsl:for-each>
        <xsl:for-each select="NET_VALUE">
          <WARTOSC_NETTO>
            <xsl:value-of select="."/>
          </WARTOSC_NETTO>
        </xsl:for-each>
        <xsl:for-each select="GROSS_VALUE">
          <WARTOSC_BRUTTO>
            <xsl:value-of select="."/>
          </WARTOSC_BRUTTO>
        </xsl:for-each>
        <WSKAZNIK_ZATWIERDZENIA>N</WSKAZNIK_ZATWIERDZENIA>
        <xsl:for-each select="SPRZEDAWCA">
          <xsl:for-each select="SPRZEDAWCA_ROW">
            <SPRZEDAWCA>
              <xsl:for-each select="SYMBOL">
                <SYMBOL>
                  <xsl:value-of select="."/>
                </SYMBOL>
              </xsl:for-each>
              <xsl:for-each select="NAZWA">
                <NAZWA>
                  <xsl:value-of select="."/>
                </NAZWA>
              </xsl:for-each>
              <xsl:for-each select="SKROT">
                <SKROT>
                  <xsl:value-of select="."/>
                </SKROT>
              </xsl:for-each>
              <xsl:for-each select="NIP">
                <NIP>
                  <xsl:value-of select="."/>
                </NIP>
              </xsl:for-each>
              <ADRES>
                <xsl:for-each select="MIEJSCOWOSC">
                  <MIEJSCOWOSC>
                    <xsl:value-of select="."/>
                  </MIEJSCOWOSC>
                </xsl:for-each>
                <xsl:for-each select="ULICA">
                  <ULICA>
                    <xsl:value-of select="."/>
                  </ULICA>
                </xsl:for-each>
                <xsl:for-each select="KOD_POCZTOWY">
                  <KOD_POCZTOWY>
                    <xsl:value-of select="."/>
                  </KOD_POCZTOWY>
                </xsl:for-each>
                <xsl:for-each select="NR_DOMU">
                  <NR_DOMU>
                    <xsl:value-of select="."/>
                  </NR_DOMU>
                </xsl:for-each>
                <xsl:for-each select="NR_LOKALU">
                  <NR_LOKALU>
                    <xsl:value-of select="."/>
                  </NR_LOKALU>
                </xsl:for-each>
              </ADRES>
            </SPRZEDAWCA>
          </xsl:for-each>
        </xsl:for-each>
        <xsl:for-each select="PLATNIK">
          <xsl:for-each select="PLATNIK_ROW">
            <PLATNIK>
              <xsl:for-each select="SYMBOL">
                <SYMBOL>
                  <xsl:value-of select="."/>
                </SYMBOL>
              </xsl:for-each>
              <xsl:for-each select="NAZWA">
                <NAZWA>
                  <xsl:value-of select="."/>
                </NAZWA>
              </xsl:for-each>
              <xsl:for-each select="SKROT">
                <SKROT>
                  <xsl:value-of select="."/>
                </SKROT>
              </xsl:for-each>
              <xsl:for-each select="NIP">
                <NIP>
                  <xsl:value-of select="."/>
                </NIP>
              </xsl:for-each>
              <ADRES>
                <xsl:for-each select="MIEJSCOWOSC">
                  <MIEJSCOWOSC>
                    <xsl:value-of select="."/>
                  </MIEJSCOWOSC>
                </xsl:for-each>
                <xsl:for-each select="ULICA">
                  <ULICA>
                    <xsl:value-of select="."/>
                  </ULICA>
                </xsl:for-each>
                <xsl:for-each select="KOD_POCZTOWY">
                  <KOD_POCZTOWY>
                    <xsl:value-of select="."/>
                  </KOD_POCZTOWY>
                </xsl:for-each>
                <xsl:for-each select="NR_DOMU">
                  <NR_DOMU>
                    <xsl:value-of select="."/>
                  </NR_DOMU>
                </xsl:for-each>
                <xsl:for-each select="NR_LOKALU">
                  <NR_LOKALU>
                    <xsl:value-of select="."/>
                  </NR_LOKALU>
                </xsl:for-each>
              </ADRES>
            </PLATNIK>
          </xsl:for-each>
        </xsl:for-each>
        <xsl:for-each select="ODBIORCA">
          <xsl:for-each select="ODBIORCA_ROW">
            <ODBIORCA>
              <xsl:for-each select="SYMBOL">
                <SYMBOL>
                  <xsl:value-of select="."/>
                </SYMBOL>
              </xsl:for-each>
              <xsl:for-each select="NAZWA">
                <NAZWA>
                  <xsl:value-of select="."/>
                </NAZWA>
              </xsl:for-each>
              <xsl:for-each select="SKROT">
                <SKROT>
                  <xsl:value-of select="."/>
                </SKROT>
              </xsl:for-each>
              <xsl:for-each select="NIP">
                <NIP>
                  <xsl:value-of select="."/>
                </NIP>
              </xsl:for-each>
              <ADRES>
                <xsl:for-each select="MIEJSCOWOSC">
                  <MIEJSCOWOSC>
                    <xsl:value-of select="."/>
                  </MIEJSCOWOSC>
                </xsl:for-each>
                <xsl:for-each select="ULICA">
                  <ULICA>
                    <xsl:value-of select="."/>
                  </ULICA>
                </xsl:for-each>
                <xsl:for-each select="KOD_POCZTOWY">
                  <KOD_POCZTOWY>
                    <xsl:value-of select="."/>
                  </KOD_POCZTOWY>
                </xsl:for-each>
                <xsl:for-each select="NR_DOMU">
                  <NR_DOMU>
                    <xsl:value-of select="."/>
                  </NR_DOMU>
                </xsl:for-each>
                <xsl:for-each select="NR_LOKALU">
                  <NR_LOKALU>
                    <xsl:value-of select="."/>
                  </NR_LOKALU>
                </xsl:for-each>
              </ADRES>
            </ODBIORCA>
          </xsl:for-each>
        </xsl:for-each>
        <POLA_DODATKOWE>
          <xsl:for-each select="SR_PARTY_DESCRIPTION">
            <PA_POLE_DODATKOWE_T>
              <NAZWA>ATRYBUT_T08</NAZWA>
              <WARTOSC>
                <xsl:value-of select="."/>
              </WARTOSC>
            </PA_POLE_DODATKOWE_T>
          </xsl:for-each>
        </POLA_DODATKOWE>
        <POZYCJE>
          <xsl:for-each select="ITEMS">
            <xsl:for-each select="ITEMS_ROW">
              <LG_ZASI_T>
                <xsl:for-each select="GUID">
                  <GUID_POZYCJI>
                    <xsl:value-of select="."/>
                  </GUID_POZYCJI>
                </xsl:for-each>
                <xsl:for-each select="ITEM_NUM">
                  <LP>
                    <xsl:value-of select="."/>
                  </LP>
                </xsl:for-each>
                <INDEKS>
                  <xsl:for-each select="SELLER_ITEM_ID">
                    <INDEKS>
                      <xsl:value-of select="."/>
                    </INDEKS>
                  </xsl:for-each>
                  <xsl:for-each select="NAME">
                    <NAZWA>
                      <xsl:value-of select="."/>
                    </NAZWA>
                  </xsl:for-each>
                </INDEKS>
                <xsl:for-each select="INMA_STVA_CODE">
                  <KOD_STAWKI_VAT>
                    <xsl:value-of select="."/>
                  </KOD_STAWKI_VAT>
                </xsl:for-each>
                <xsl:for-each select="QUANTITY_VALUE">
                  <ILOSC>
                    <xsl:value-of select="."/>
                  </ILOSC>
                </xsl:for-each>
                <xsl:for-each select="CURRENCY">
                  <KOD_WALUTY>
                    <xsl:value-of select="."/>
                  </KOD_WALUTY>
                </xsl:for-each>
                <xsl:for-each select="UNIT_PRICE_VALUE">
                  <CENA>
                    <xsl:value-of select="."/>
                  </CENA>
                </xsl:for-each>
                <xsl:for-each select="UNIT_PRICE_BASE">
                  <CENA_Z_CENNIKA>
                    <xsl:value-of select="."/>
                  </CENA_Z_CENNIKA>
                </xsl:for-each>
                <xsl:for-each select="UNIT_PRICE_BASE">
                  <CENA_Z_CENNIKA_WAL>
                    <xsl:value-of select="."/>
                  </CENA_Z_CENNIKA_WAL>
                </xsl:for-each>
                <xsl:for-each select="DISCOUNT_VALUE">
                  <OPUST_NA_POZYCJI>
                    <xsl:value-of select="."/>
                  </OPUST_NA_POZYCJI>
                </xsl:for-each>
                <POLA_DODATKOWE>
                  <xsl:for-each select="PROMOTION_CODE">
                    <PA_POLE_DODATKOWE_T>
                      <NAZWA>ATRYBUT_T01</NAZWA>
                      <WARTOSC>
                        <xsl:value-of select="."/>
                      </WARTOSC>
                    </PA_POLE_DODATKOWE_T>
                  </xsl:for-each>
                  <xsl:for-each select="PROMOTION_NAME">
                    <PA_POLE_DODATKOWE_T>
                      <NAZWA>ATRYBUT_T02</NAZWA>
                      <WARTOSC>
                        <xsl:value-of select="."/>
                      </WARTOSC>
                    </PA_POLE_DODATKOWE_T>
                  </xsl:for-each>
                  <xsl:for-each select="DESCRIPTION">
                    <PA_POLE_DODATKOWE_T>
                      <NAZWA>ATRYBUT_T03</NAZWA>
                      <WARTOSC>
                        <xsl:value-of select="."/>
                      </WARTOSC>
                    </PA_POLE_DODATKOWE_T>
                  </xsl:for-each>
                </POLA_DODATKOWE>
                <xsl:for-each select="UNIT_OF_MEASURE">
                  <NAZWA_JEDNOSTKI_MIARY>
                    <xsl:value-of select="."/>
                  </NAZWA_JEDNOSTKI_MIARY>
                </xsl:for-each>
              </LG_ZASI_T>
            </xsl:for-each>
          </xsl:for-each>
        </POZYCJE>
      </xsl:for-each>
    </LG_ZASP_T>
  </xsl:template>
</xsl:stylesheet>';

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
            v_xslt,
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
                   'SELECT zare.dest_symbol order_id,
                           sord.doc_symbol_rcv AS sfa_salo_id,
                           zare.data_realizacji realization_date,
                           inma.indeks commoditiy_id,
                           jg_output_sync.format_number(zare.ilosc, 4) quantity_ordered,
                           jg_output_sync.format_number(reze.ilosc_zarezerwowana + reze.ilosc_pobrana, 100) quantity_reserved
                      FROM lg_rzm_rezerwacje reze,
                           lg_rzm_zadania_rezerwacji zare,
                           ap_indeksy_materialowe inma,
                           lg_sal_orders sord,
                           lg_sal_orders_it sori
                     WHERE     reze.zare_id  = zare.id
                           AND zare.inma_id  = inma.id
                           AND zare.zrre_id  = sori.id
                           AND sord.id       = sori.document_id
                           AND zare.zrre_typ = ''ZASI''
                           AND reze.id = IN (:p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:template match="@*|node()">
                        <xsl:copy>
                           <xsl:apply-templates select="@*|node()" />
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <RESERVATION><xsl:apply-templates/></RESERVATION>
                     </xsl:template>
                     <xsl:template priority="2" match="RESERVATIONS/RESERVATION">
                        <RESERVATION><xsl:apply-templates/></RESERVATION>
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/reservations',
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
                   'SETS_COMPONENTS',
                   'SELECT inma_kpl.indeks set_id,
       inma_kpl.nazwa set_name,
       jg_output_sync.format_number (
           lg_stm_sgpu_sql.stan_goracy (inma_kpl.id,
                                        inma_kpl.jdmr_nazwa,
                                        NULL),
           100)
           available_stock,
       jg_output_sync.format_number (inma_kpl.atrybut_n05, 4)
           price_before_discount,
       jg_output_sync.format_number (inma_kpl.atrybut_n06, 4)
           price_after_discount,
       inma_kpl.atrybut_d01 valid_date,
       inma_kpl.aktualny up_to_date,
       CURSOR (
           SELECT inma_skpl.indeks commodity_id,
                  inma_skpl.nazwa commodity_name,
                  jg_output_sync.format_number (kpsk1.ilosc, 100) quantity,
                  kpsk1.premiowy bonus,
                  DECODE (kpsk1.dynamiczny, ''T'', ''DYNAMIC'', ''STATIC'')
                      set_type,
                  DECODE (inma_skpl.atrybut_t03, ''T'', ''N'', ''Y'')
                      contract_payment,
                  inma_skpl.aktualny up_to_date,
                  CURSOR (
                      SELECT indeks commodity_id,
                             nazwa commodity_name,
                             inma.aktualny up_to_date
                        FROM ap_indeksy_materialowe inma
                       WHERE inma.id IN (SELECT /*+ DYNAMIC_SAMPLING(a, 5) */
                                                COLUMN_VALUE
                                           FROM TABLE (
                                                    jg_dynamic_set_commponents (
                                                        kpsk1.id)) a))
                      dynamic_components
             FROM lg_kpl_skladniki_kompletu kpsk1,
                  ap_indeksy_materialowe inma_skpl
            WHERE     kpsk1.skl_inma_id = inma_skpl.id
                  AND kpsk1.kpl_inma_id = inma_kpl.id)
           components
  FROM ap_indeksy_materialowe inma_kpl
 WHERE inma_kpl.id IN ( :p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <SET_COMPONENTS><xsl:apply-templates/></SET_COMPONENTS>
                     </xsl:template>
                     <xsl:template priority="2" match="COMPONENTS/COMPONENTS_ROW">
                        <COMPONENT><xsl:apply-templates/></COMPONENT>
                     </xsl:template>
                     <xsl:template priority="2" match="DYNAMIC_COMPONENTS/DYNAMIC_COMPONENTS_ROW">
                        <DYNAMIC_COMPONENT><xsl:apply-templates/></DYNAMIC_COMPONENT>
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
        VALUES (
                   jg_sqre_seq.NEXTVAL,
                   'SALES_REPRESENTATIVES',
                   'SELECT okgi.id,
       osby.imie || '' '' || osby.nazwisko AS name,
       osol.atrybut_t02 AS numer_kasy,
       osol.kod AS id_erp,
       1 AS active,
       TRANSLATE (osby.imie || ''.'' || osby.nazwisko || ''.JBS'',
                  ''ĄąĆćĘęŁłŃńÓóŚśŹźŻż'',
                  ''AaCcEeLlNnOoSsZzZz'')
           AS userlogin,
       osby.imie username,
       osby.nazwisko usersurname,
       TRANSLATE (
           SUBSTR (osby.imie, 1, 1) || ''.'' || osby.nazwisko || ''@GOLDWELL.PL'',
           ''ĄąĆćĘęŁłŃńÓóŚśŹźŻż'',
           ''AaCcEeLlNnOoSsZzZz'')
           AS useremail,
       okgi.atrybut_t02 AS userphone,
       okgi.id AS area_id,
       CURSOR (
           SELECT konr.symbol customer_number
             FROM ap_kontrahenci konr,
                  lg_kontrahenci_grup kngr,
                  (SELECT *
                     FROM lg_grupy_kontrahentow
                   START WITH id = 63
                   CONNECT BY PRIOR id = grkn_id) grkn
            WHERE     kngr.konr_id = konr.id
                  AND kngr.grkn_id = grkn.id
                  AND grkn.nazwa = osol.atrybut_t01)
           contractors
  FROM lg_osoby_log osol, pa_osoby osby, ap_okregi_sprzedazy okgi
 WHERE     okgi.symbol = osol.atrybut_t01
       AND osol.id IN (:p_id)
       AND osol.osby_id = osby.id',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
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

    v_xslt :=
        '<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml" encoding="windows-1250" indent="yes"/>
  <xsl:template match="/">
    <PA_KONTRAHENT_TK xmlns="http://www.teta.com.pl/teta2000/kontrahent-1" wersja="1.0">
      <xsl:for-each select="NewCustomer">
        <xsl:for-each select="BasicData">
          <xsl:for-each select="MobizID">
            <SYMBOL>
              <xsl:value-of select="."/>
            </SYMBOL>
          </xsl:for-each>
          <xsl:for-each select="Name">
            <NAZWA>
              <xsl:value-of select="."/>
            </NAZWA>
          </xsl:for-each>
          <xsl:for-each select="Shortcut">
            <SKROT>
              <xsl:value-of select="."/>
            </SKROT>
          </xsl:for-each>
          <xsl:for-each select="TIN">
            <NIP>
              <xsl:value-of select="."/>
            </NIP>
          </xsl:for-each>
          <ADRES>
            <xsl:for-each select="Address">
              <xsl:for-each select="City">
                <MIEJSCOWOSC>
                  <xsl:value-of select="."/>
                </MIEJSCOWOSC>
              </xsl:for-each>
              <xsl:for-each select="Street">
                <ULICA>
                  <xsl:value-of select="."/>
                </ULICA>
              </xsl:for-each>
              <xsl:for-each select="Postcode">
                <KOD_POCZTOWY>
                  <xsl:value-of select="."/>
                </KOD_POCZTOWY>
              </xsl:for-each>
            </xsl:for-each>
            <xsl:for-each select="Phone">
              <NR_TEL>
                <VARCHAR2>
                  <xsl:value-of select="."/>
                </VARCHAR2>
              </NR_TEL>
            </xsl:for-each>
            <xsl:for-each select="Fax">
              <NR_FAX>
                <VARCHAR2>
                  <xsl:value-of select="."/>
                </VARCHAR2>
              </NR_FAX>
            </xsl:for-each>
            <ADRESY_EMAIL>
              <xsl:for-each select="Email">
                <VARCHAR2>
                  <xsl:value-of select="."/>
                </VARCHAR2>
              </xsl:for-each>
            </ADRESY_EMAIL>
            <RegionID>080</RegionID>
            <ProvinceID>450</ProvinceID>
          </ADRES>
          <ClassID>Detal</ClassID>
          <Profile>Reseller</Profile>
          <ContactPerson>Tomasz Wspaniały</ContactPerson>
          <ChainID>123</ChainID>
        </xsl:for-each>
        <AdditionalData>
          <SalesRepresentativeID>5235</SalesRepresentativeID>
        </AdditionalData>
        <PLATNIK_VAT>T</PLATNIK_VAT>
        <BLOKADA_ZAKUPU>N</BLOKADA_ZAKUPU>
        <RODZAJ_DATY_WAR_HANDL_FAKT>S</RODZAJ_DATY_WAR_HANDL_FAKT>
        <RODZAJ_DATY_WAR_HANDL_ZAM>W</RODZAJ_DATY_WAR_HANDL_ZAM>
        <RODZAJ_DATY_TERM_PLAT_FS>DW</RODZAJ_DATY_TERM_PLAT_FS>
        <GRUPY_KONTRHENTA/>
        <JEDNOSTKI_OSOBY/>
      </xsl:for-each>
    </PA_KONTRAHENT_TK>
  </xsl:template>
</xsl:stylesheet>';

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
    VALUES (jg_sqre_seq.NEXTVAL,
            'NEW_CONTRACTORS',
            'SELECT osol.kod        id,
                         osby.code       name,
                         osol.aktualna   active,
                         osol.first_name username,
                         osol.surname    usersurname,
                         CURSOR (SELECT konr.symbol customerid
                                   FROM lg_osoby_log osol1,
                                        (SELECT *
                                           FROM lg_grupy_kontrahentow
                                          START WITH id = 63
                                        CONNECT BY PRIOR id = grkn_id) grko,
                                        lg_kontrahenci_grup kngr,
                                        ap_kontrahenci konr
                                  WHERE     osol1.atrybut_t01 = grko.nazwa
                                        AND grko.id = kngr.grkn_id
                                        AND osol1.aktualna = ''T''
                                        AND kngr.konr_id = konr.id
                                        AND osol1.id = osol.id) customers
                     FROM lg_osoby_log osol, pa_osoby osby
                    WHERE     osol.atrybut_t01 IS NOT NULL
                          AND osol.osby_id = osby.id
                          AND osol.id IN (:p_id)',
            v_xslt,
            'OUT/new_customer',
            'T',
            'IN');

    v_xslt :=
        '<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml" encoding="windows-1250" indent="yes"/>
  <xsl:template match="/">
    <PA_KONTRAHENT_TK xmlns="http://www.teta.com.pl/teta2000/kontrahent-1" wersja="1.0">
      <xsl:for-each select="CustomerData">
        <xsl:for-each select="BasicData">
          <xsl:for-each select="MobizID">
            <SYMBOL>
              <xsl:value-of select="."/>
            </SYMBOL>
          </xsl:for-each>
          <xsl:for-each select="Name">
            <NAZWA>
              <xsl:value-of select="."/>
            </NAZWA>
          </xsl:for-each>
          <xsl:for-each select="Shortcut">
            <SKROT>
              <xsl:value-of select="."/>
            </SKROT>
          </xsl:for-each>
          <xsl:for-each select="TIN">
            <NIP>
              <xsl:value-of select="."/>
            </NIP>
          </xsl:for-each>
          <ADRES>
            <xsl:for-each select="Address">
              <xsl:for-each select="City">
                <MIEJSCOWOSC>
                  <xsl:value-of select="."/>
                </MIEJSCOWOSC>
              </xsl:for-each>
              <xsl:for-each select="Street">
                <ULICA>
                  <xsl:value-of select="."/>
                </ULICA>
              </xsl:for-each>
              <xsl:for-each select="Postcode">
                <KOD_POCZTOWY>
                  <xsl:value-of select="."/>
                </KOD_POCZTOWY>
              </xsl:for-each>
            </xsl:for-each>
            <xsl:for-each select="Phone">
              <NR_TEL>
                <VARCHAR2>
                  <xsl:value-of select="."/>
                </VARCHAR2>
              </NR_TEL>
            </xsl:for-each>
            <xsl:for-each select="Fax">
              <NR_FAX>
                <VARCHAR2>
                  <xsl:value-of select="."/>
                </VARCHAR2>
              </NR_FAX>
            </xsl:for-each>
            <ADRESY_EMAIL>
              <xsl:for-each select="Email">
                <VARCHAR2>
                  <xsl:value-of select="."/>
                </VARCHAR2>
              </xsl:for-each>
            </ADRESY_EMAIL>
            <RegionID>080</RegionID>
            <ProvinceID>450</ProvinceID>
          </ADRES>
          <ClassID>Detal</ClassID>
          <Profile>Reseller</Profile>
          <ContactPerson>Tomasz Wspaniały</ContactPerson>
          <ChainID>123</ChainID>
        </xsl:for-each>
        <AdditionalData>
          <SalesRepresentativeID>5235</SalesRepresentativeID>
        </AdditionalData>
        <PLATNIK_VAT>T</PLATNIK_VAT>
        <BLOKADA_ZAKUPU>N</BLOKADA_ZAKUPU>
        <RODZAJ_DATY_WAR_HANDL_FAKT>S</RODZAJ_DATY_WAR_HANDL_FAKT>
        <RODZAJ_DATY_WAR_HANDL_ZAM>W</RODZAJ_DATY_WAR_HANDL_ZAM>
        <RODZAJ_DATY_TERM_PLAT_FS>DW</RODZAJ_DATY_TERM_PLAT_FS>
        <GRUPY_KONTRHENTA/>
        <JEDNOSTKI_OSOBY/>
      </xsl:for-each>
    </PA_KONTRAHENT_TK>
  </xsl:template>
</xsl:stylesheet>';

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
    VALUES (jg_sqre_seq.NEXTVAL,
            'CUSTOMER_DATA',
            'SELECT osol.kod        id,
                         osby.code       name,
                         osol.aktualna   active,
                         osol.first_name username,
                         osol.surname    usersurname,
                         CURSOR (SELECT konr.symbol customerid
                                   FROM lg_osoby_log osol1,
                                        (SELECT *
                                           FROM lg_grupy_kontrahentow
                                          START WITH id = 63
                                        CONNECT BY PRIOR id = grkn_id) grko,
                                        lg_kontrahenci_grup kngr,
                                        ap_kontrahenci konr
                                  WHERE     osol1.atrybut_t01 = grko.nazwa
                                        AND grko.id = kngr.grkn_id
                                        AND osol1.aktualna = ''T''
                                        AND kngr.konr_id = konr.id
                                        AND osol1.id = osol.id) customers
                     FROM lg_osoby_log osol, pa_osoby osby
                    WHERE     osol.atrybut_t01 IS NOT NULL
                          AND osol.osby_id = osby.id
                          AND osol.id IN (:p_id)',
            v_xslt,
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
        VALUES (
                   jg_sqre_seq.NEXTVAL,
                   'DELIVERY_METHODS',
                   'SELECT kod delivery_method_code,
                         opis description,
                         aktualna up_to_date
                    FROM ap_sposoby_dostaw spdo
                   WHERE spdo.id IN (:p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                  <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                  <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <DELIVERY_METHOD><xsl:apply-templates /></DELIVERY_METHOD>
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
        VALUES (
                   jg_sqre_seq.NEXTVAL,
                   'PAYMENTS_METHODS',
                   'SELECT foza.kod payment_method_code,
                         foza.opis description,
                         odroczenie_platnosci deferment_of_payment,
                         (SELECT rv_meaning
                            FROM cg_ref_codes
                           WHERE     rv_domain = ''FORMY_ZAPLATY''
                                 AND rv_low_value = foza.typ) payment_type,
                          aktualna up_to_date
                    FROM ap_formy_zaplaty foza
                   WHERE foza.id IN (:p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <PAYMENT_METHOD><xsl:apply-templates /></PAYMENT_METHOD>
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
        VALUES (
                   jg_sqre_seq.NEXTVAL,
                   'ORDERS_PATTERNS',
                   'SELECT wzrc.pattern pattern_code,
                         wzrc.name pattern_name,
                         wzrc.up_to_date
                    FROM lg_documents_templates wzrc
                   WHERE     document_type = ''ZS''
                         AND wzrc.id IN (:p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <ORDER_PATTERN><xsl:apply-templates /></ORDER_PATTERN>
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/orders_patterns',
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
                   'DISCOUNTS',
                   'WITH upta
     AS (SELECT upta.symbol,
                wakw.id AS wakw_id,
                upta.id AS upta_id,
                krwy.typ_wykorzystania
           FROM lg_upusty_tabelaryczne upta
                JOIN lg_kryteria_wykorzystane krwy ON upta.id = krwy.upta_id
                JOIN lg_wartosci_kryt_wyk wakw ON wakw.krwy_id = krwy.id
          WHERE upta.symbol LIKE ''UG%''),
     upta_koup
     AS (SELECT a.upta_id, koup.upust_procentowy
           FROM (SELECT *
                   FROM upta
                        PIVOT
                            (MAX (wakw_id) wakw_id
                            FOR typ_wykorzystania
                            IN (''W'' AS "W", ''K'' AS "K"))) a
                JOIN lg_komorki_upustow koup
                    ON     koup.wakw_id_kolumna = a.k_wakw_id
                       AND koup.wakw_id_wiersz = a.w_wakw_id)
SELECT upta.symbol discount_number,
       inma.indeks item_index,
       konr.symbol customer_number,
       gras.kod commodity_group_code,
       grod.grupa reciever_group,
       prup.data_od date_from,
       NVL (prup.data_do, TO_DATE (''2049/12/31'', ''YYYY/MM/DD'')) date_to,
       jg_output_sync.format_number (
           NVL (upko.upust_procentowy, prup.upust_procentowy),
           100)
           percent_discount
  FROM lg_przyp_upustow prup
       INNER JOIN lg_upusty_tabelaryczne upta ON prup.upta_id = upta.id
       LEFT JOIN ap_indeksy_materialowe inma ON inma.id = prup.inma_id
       LEFT JOIN ap_kontrahenci konr ON konr.id = prup.konr_id
       LEFT JOIN ap_grupy_asortymentowe gras ON gras.id = prup.gras_id
       LEFT JOIN ap_grupy_odbiorcow grod ON grod.id = prup.grod_id
       LEFT JOIN upta_koup upko ON prup.upta_id = upko.upta_id
 WHERE     (   prup.upust_procentowy IS NOT NULL
            OR upko.upust_procentowy IS NOT NULL)
       AND SYSDATE BETWEEN prup.data_od AND NVL (data_do, SYSDATE)
       AND prup.id IN (:p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <DISCOUNT><xsl:apply-templates/></DISCOUNT>
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/discounts',
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
                   'WAREHOUSES',
                   'SELECT maga.kod id,
       maga.nazwa name,
       CURSOR (
           SELECT inma1.indeks commodity_id,
                  jg_output_sync.format_number (sum(stma1.stan_goracy), 100)
                      quantity
             FROM ap_stany_magazynowe stma1,
                  ap_indeksy_materialowe inma1,
                  ap_magazyny maga1
            WHERE     inma1.id = stma1.suob_inma_id
                  AND maga1.id = stma1.suob_maga_id
                  AND stma1.suob_inma_id in (SELECT suob_inma_id from ap_stany_magazynowe stma where stma.id IN (:p_id))
                  AND maga1.kod = maga.kod
                  AND maga1.id in (SELECT suob_maga_id from ap_stany_magazynowe stma where stma.id IN (:p_id))
                  group by inma1.indeks 
                  )
           stocks
  FROM ap_stany_magazynowe stma, ap_magazyny maga
WHERE     stma.suob_maga_id = maga.id
       AND (kod LIKE ''1__'' OR kod in (''500'',''300''))
       AND stma.suob_inma_id in (SELECT suob_inma_id from ap_stany_magazynowe stma where stma.id IN (:p_id))
       AND stma.suob_maga_id in (SELECT suob_maga_id from ap_stany_magazynowe stma where stma.id IN (:p_id))
GROUP BY maga.kod, maga.nazwa',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                    <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                    <xsl:strip-space elements="*"/>
                    <xsl:template match="node()|@*">
                       <xsl:copy>
                          <xsl:apply-templates select="node()|@*"/>
                       </xsl:copy>
                    </xsl:template>
                    <xsl:template match="*[not(@*|comment()|processing-instruction()) and normalize-space()='''']"/>
                    <xsl:template priority="2" match="ROW">
                       <WAREHOUSE><xsl:apply-templates/></WAREHOUSE>
                    </xsl:template>
                    <xsl:template priority="2" match="STOCKS/STOCKS_ROW">
                       <STOCK><xsl:apply-templates/></STOCK>
                    </xsl:template>
                 </xsl:stylesheet>',
                   'IN/warehouses',
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
                   'CONTRACTS',
                   'SELECT umsp.symbol id,
       konr.symbol contractor_id,
       umsp1.date_from,
       umsp.data_do date_to,
       wzrc.nazwa contract_destination,
       jg_output_sync.format_number (umsp1.contract_value, 10) contract_value,
       jg_output_sync.format_number (
          umsp1.splata,
           10)
           contract_value_realized,
       CASE
           WHEN   umsp1.splata
                -   (umsp1.contract_value / umsp1.duration)
                  * (FLOOR (MONTHS_BETWEEN (SYSDATE, umsp1.date_from))) < 0
           THEN
              ''T''
           ELSE
               ''N''
       END
           delayed,
       CASE
           WHEN     (umsp1.contract_value / umsp1.duration)
                  * months_passed
                - umsp1.splata > (umsp1.contract_value / umsp1.duration) * 3
           THEN
               ''N''
           ELSE
               ''T''
       END
           can_skip_repayment,
  round(umsp1.contract_value/umsp1.duration,2) as monthly_installment,
  round(greatest(umsp1.splata-umsp1.contract_value,least(0,umsp1.splata - umsp1.contract_value/umsp1.duration*months_passed)),2) debt
  FROM lg_ums_umowy_sprz umsp,
       ap_kontrahenci konr,
       lg_wzorce wzrc,
       (SELECT id,
                (SELECT Lg_Ums_Umsi_Def.Wartosc(UMSI.ID)
                  FROM 
                       lg_ums_umowy_sprz_it umsi
                 WHERE umsi.umsp_id = umsp.id)
                   contract_value,
               (SELECT NVL (SUM (umru.wartosc), 0)
                  FROM lg_ums_realizacje_umsi umru, lg_ums_umowy_sprz_it umsi
                 WHERE umsi.id = umru.uiwl_id AND umsi.umsp_id = umsp.id)
                   splata,
               NVL (ADD_MONTHS (umsp.data_do, -umsp.atrybut_n01) + 1,
                    umsp.data_od)
                   AS date_from,
               nvl(umsp.atrybut_n01,round(months_between(umsp.data_do,umsp.data_od))) duration,
               floor(months_between(sysdate,NVL (ADD_MONTHS (umsp.data_do, -umsp.atrybut_n01) + 1,
                    umsp.data_od))) as months_passed
          FROM lg_ums_umowy_sprz umsp
          WHERE umsp.data_wystawienia >= to_date(''2016/01/01'',''YYYY/MM/DD'')
          AND umsp.zamknieta = ''N''
          ) umsp1
 WHERE konr.id = umsp.konr_id_pl AND wzrc.id = umsp.wzrc_id AND umsp.id = umsp1.id  AND umsp.id IN ( :p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:template match="@*|node()">
                        <xsl:copy>
                           <xsl:apply-templates select="@*|node()" />
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <CONTRACT><xsl:apply-templates/></CONTRACT>            
                     </xsl:template>
                     <xsl:template priority="2" match="LINES/LINES_ROW">
                        <LINE><xsl:apply-templates/></LINE>            
                     </xsl:template>
                     <xsl:template priority="2" match="PERIODS/PERIODS_ROW">
                        <PERIOD><xsl:apply-templates/></PERIOD>            
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/contracts',
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
                   'SUPPORT_FUNDS',
                   'SELECT fwk.konr_symbol AS client_symbol,
       SUM (fwk.fwk_m_pozostalo) AS marketing_support_fund,
       SUM (fwk.fwk_t_pozostalo) AS real_support_fund,
       SUM (fwk.fwk_m_pozostalo) + SUM (fwk.fwk_t_pozostalo)
           AS sum_support_fund
  FROM jbs_mp_przeglad_fwk fwk
 WHERE     fwk.data_faktury >= ADD_MONTHS (TRUNC (SYSDATE, ''MM''), -12)
       --AND fwk.czy_zaplacona = ''T''
       AND fwk.konr_symbol IN (SELECT konr_symbol
                                 FROM jbs_mp_przeglad_fwk
                                WHERE id IN (:p_id))
GROUP BY fwk.konr_symbol',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <SUPPORT_FUND><xsl:apply-templates/></SUPPORT_FUND>
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/support_funds',
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
                   'LOYALITY_POINTS',
                   'SELECT puko.CLIENT_SYMBOL,
                         puko.POINTS_TYPE,
                         puko.POINTS_VALUE,
                         puko.SUM_REAL_POINTS_VALUE,
                         puko.SUM_TEMPORARY_POINTS_VALUE,
                         puko.SUM_POINTS_VALUE,
                         puko.CALCULATION_DATE,
                         ADD_MONTHS(puko.CALCULATION_DATE, 24) EXPIRE_DATE,
                         CURSOR (SELECT DECODE(puko1.rzeczywiste, ''T'', ''RZECZYWISTE'', ''TYMCZASOWE'') AS POINTS_TYPE,
                                        puko1.wartosc_punktow             AS POINTS_VALUE,
                                        dosp.data_faktury                 AS CALCULATION_DATE,
                                        ADD_MONTHS(dosp.data_faktury, 24) AS EXPIRE_DATE
                                   FROM lg_plo_punkty_kontrahenta puko1,
                                        lg_dokumenty_sprz_vw dosp
                                  WHERE     dosp.id = puko1.dosp_id
                                        AND puko1.konr_id = puko.konr_id
                               ORDER BY puko1.id DESC)                    AS HISTORY
                    FROM (SELECT konr.symbol  AS CLIENT_SYMBOL,
                                 DECODE(puko.rzeczywiste, ''T'', ''RZECZYWISTE'', ''TYMCZASOWE'') AS POINTS_TYPE,
                                 puko.wartosc_punktow AS POINTS_VALUE,
                                 (SELECT SUM(puko1.wartosc_punktow)
                                    FROM lg_plo_punkty_kontrahenta puko1
                                   WHERE     puko1.konr_id = puko.konr_id
                                         AND puko1.rzeczywiste = ''T'') AS SUM_REAL_POINTS_VALUE,
                                 (SELECT SUM(puko1.wartosc_punktow)
                                    FROM lg_plo_punkty_kontrahenta puko1
                                   WHERE     puko1.konr_id = puko.konr_id
                                         AND puko1.rzeczywiste = ''N'') AS SUM_TEMPORARY_POINTS_VALUE, 
                                 (SELECT SUM(puko1.wartosc_punktow)
                                    FROM lg_plo_punkty_kontrahenta puko1
                                   WHERE puko1.konr_id = puko.konr_id) AS SUM_POINTS_VALUE,                
                                 NVL((SELECT CASE WHEN (SELECT MAX(id) FROM lg_plo_punkty_kontrahenta WHERE konr_id = puko.konr_id) = dosp.puko_id THEN dosp.data_faktury ELSE dosp.data_faktury + 1 END
                                        FROM (SELECT puko1.id puko_id,
                                                     puko1.konr_id konr_id,
                                                     dosp.data_faktury data_faktury                                     
                                                FROM lg_plo_punkty_kontrahenta puko1,
                                                     lg_dokumenty_sprz_vw dosp
                                               WHERE     dosp.id = puko1.dosp_id
                                                     AND puko1.dosp_id IS NOT NULL
                                            ORDER BY puko1.id desc) dosp
                                       WHERE     dosp.konr_id = puko.konr_id
                                             AND dosp.data_faktury IS NOT NULL
                                             AND ROWNUM = 1), TO_DATE(''01-01-2000'', ''DD-MM-YYYY'')) CALCULATION_DATE,
                                 puko.konr_id
                           FROM lg_plo_punkty_kontrahenta puko,
                                ap_kontrahenci konr
                          WHERE     konr.id = puko.konr_id
                                AND puko.id IN (:p_id)) puko',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <LOYALITY_POINT><xsl:apply-templates/></LOYALITY_POINT>
                     </xsl:template>
                     <xsl:template priority="2" match="HISTORY/HISTORY_ROW">
                        <POINTS><xsl:apply-templates/></POINTS>            
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/loyality_points',
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
                   'TRADE_CONTRACTS_INDIVIDUAL',
                   'SELECT konr.symbol contractors_id, 
        konr.nr_umowy_ind AS contract_number,
       DECODE (individual_contract,
               ''T'', konr.data_umowy_ind,
               konr.atrybut_d01)
           AS contract_date,
       individual_contract AS individual_contract,
       konr.foza_kod AS default_payment_type,
       NVL (konr.limit_kredytowy, 0) AS credit_limit,
       konr.dni_do_zaplaty AS payment_date,
       prup.upust_procentowy AS discount_percent,
       a_mp_dekoduj_pkt (konr.atrybut_t07, 0) AS quarter_points,
       a_mp_dekoduj_pkt (konr.atrybut_t07, 1) AS half_year_points,
       a_mp_dekoduj_pkt (konr.atrybut_t07, 2) AS year_points,
       a_mp_dekoduj_pkt (konr.atrybut_t07, 3) AS quarter_discount,
       a_mp_dekoduj_pkt (konr.atrybut_t07, 4) AS half_year_discount,
       a_mp_dekoduj_pkt (konr.atrybut_t07, 5) AS year_discount,
       konr.atrybut_n05 AS quarter_threshold,
       konr.atrybut_n02 AS half_year_threshold,
       konr.atrybut_n03 AS year_threshold,
       DECODE (
           (SELECT COUNT (*)
              FROM lg_przyp_upustow prup1
                   JOIN lg_upusty_tabelaryczne upta1
                       ON prup1.upta_id = upta1.id
             WHERE     upta1.symbol = ''SKONTO''
                   AND SYSDATE BETWEEN prup1.data_od
                                   AND NVL (prup1.data_do, SYSDATE)
                   AND prup.konr_id = konr.id),
           0, ''N'',
           ''T'')
           skonto
  FROM (SELECT CASE
                   WHEN konr.atrybut_t05 LIKE ''%UM IND%'' THEN ''T''
                   ELSE ''N''
               END
                   individual_contract,
               konr.*
          FROM ap_kontrahenci konr) konr,
       lg_przyp_upustow prup
 WHERE prup.grod_id(+) = konr.grod_id AND konr.id IN ( :p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <TRADE_CONTRACTS><xsl:apply-templates/></TRADE_CONTRACTS>
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/trade_contracts',
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
                   'TRADE_CONTRACTS',
                   'WITH ind_co
     AS (SELECT konr.id AS konr_id,
                DECODE (individual_contract,
                        ''T'', a_mp_dekoduj_pkt (konr.atrybut_t07, 0),
                        atrybut_n04)
                    AS quarter_points,
                DECODE (individual_contract,
                        ''T'', a_mp_dekoduj_pkt (konr.atrybut_t07, 1),
                        NULL)
                    AS half_year_points,
                DECODE (individual_contract,
                        ''T'', a_mp_dekoduj_pkt (konr.atrybut_t07, 2),
                        NULL)
                    AS year_points,
                DECODE (individual_contract,
                        ''T'', a_mp_dekoduj_pkt (konr.atrybut_t07, 3),
                        NULL)
                    AS quarter_discount,
                DECODE (individual_contract,
                        ''T'', a_mp_dekoduj_pkt (konr.atrybut_t07, 4),
                        NULL)
                    AS half_year_discount,
                DECODE (individual_contract,
                        ''T'', a_mp_dekoduj_pkt (konr.atrybut_t07, 5),
                        NULL)
                    AS year_discount,
                DECODE (individual_contract, ''T'', konr.atrybut_n05, NULL)
                    AS quarter_threshold,
                DECODE (individual_contract, ''T'', konr.atrybut_n02, NULL)
                    AS half_year_threshold,
                DECODE (individual_contract, ''T'', konr.atrybut_n03, NULL)
                    AS year_threshold
           FROM (SELECT CASE
                            WHEN konr.atrybut_t05 LIKE ''%UM IND%'' THEN ''T''
                            ELSE ''N''
                        END
                            individual_contract,
                        konr.*
                   FROM ap_kontrahenci konr
                  WHERE konr.platnik = ''T'') konr),
     ind_co_data
     AS (SELECT *
           FROM ind_co
                UNPIVOT
                    (quantity
                    FOR col_name
                    IN (quarter_points,
                       half_year_points,
                       year_points,
                       quarter_discount,
                       half_year_discount,
                       year_discount,
                       quarter_threshold,
                       half_year_threshold,
                       year_threshold)))
SELECT konr.symbol contractors_id,
       konr.nr_umowy_ind AS contract_number,
       DECODE (individual_contract,
               ''T'', konr.data_umowy_ind,
               konr.atrybut_d01)
           AS contract_date,
       individual_contract AS individual_contract,
       konr.foza_kod AS default_payment_type,
       NVL (konr.limit_kredytowy, 0) AS credit_limit,
       konr.dni_do_zaplaty AS payment_date,
       SUBSTR (grod.grupa, 2, 2) AS discount_percent,
       CURSOR (
           SELECT col_name,
                  jg_output_sync.format_number (quantity, 2) quantity
             FROM ind_co_data icd
            WHERE quantity IS NOT NULL AND icd.konr_id = konr.id)
           AS bonus_points,
       DECODE (
           (SELECT COUNT (*)
              FROM lg_przyp_upustow prup1
                   JOIN lg_upusty_tabelaryczne upta1
                       ON prup1.upta_id = upta1.id
             WHERE     upta1.symbol = ''SKONTO''
                   AND SYSDATE BETWEEN prup1.data_od
                                   AND NVL (prup1.data_do, SYSDATE)
                   AND prup1.konr_id = konr.id),
           0, ''N'',
           ''T'')
           skonto
  FROM (SELECT CASE
                   WHEN konr.atrybut_t05 LIKE ''%UM IND%'' THEN ''T''
                   ELSE ''N''
               END
                   individual_contract,
               konr.*
          FROM ap_kontrahenci konr) konr,
       ap_grupy_odbiorcow grod
 WHERE     grod.id(+) = konr.grod_id
       AND konr.platnik = ''T''
       AND konr.id IN (:p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <TRADE_CONTRACTS><xsl:apply-templates/></TRADE_CONTRACTS>
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/trade_contracts',
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
                   'DELIVERIES',
                   'SELECT doob.symbol AS document_symbol,
       doob.konr_symbol AS contractor_symbol,
       doob.data_realizacji AS realization_date,
       doob.numer AS document_number,
       doob.numer_zamowienia AS order_symbol,
       (SELECT cono.tracking_number
          FROM ap_dokumenty_obrot doob1
               JOIN lg_specyf_wysylki_doob swdo ON swdo.doob_id = doob1.id
               JOIN lg_specyf_wysylki_opak spwo
                   ON spwo.spws_id = swdo.spws_id
               JOIN lg_trs_source_documents sodo ON sodo.doc_id = spwo.id
               JOIN lg_trs_sodo_shun sosh ON sosh.sodo_id = sodo.id
               JOIN lg_trs_shipping_units shun ON shun.id = sosh.shun_id
               JOIN lg_trs_consignment_notes cono
                   ON cono.id = shun.cono_id AND cono.status <> ''OP''
         WHERE doob1.id = doob.id)
           AS tracking_number,
       (SELECT cono.tracking_link
          FROM ap_dokumenty_obrot doob1
               JOIN lg_specyf_wysylki_doob swdo ON swdo.doob_id = doob1.id
               JOIN lg_specyf_wysylki_opak spwo
                   ON spwo.spws_id = swdo.spws_id
               JOIN lg_trs_source_documents sodo ON sodo.doc_id = spwo.id
               JOIN lg_trs_sodo_shun sosh ON sosh.sodo_id = sodo.id
               JOIN lg_trs_shipping_units shun ON shun.id = sosh.shun_id
               JOIN lg_trs_consignment_notes cono
                   ON cono.id = shun.cono_id AND cono.status <> ''OP''
         WHERE doob1.id = doob.id)
           AS tracking_link,
       CURSOR (SELECT dobi.numer AS ordinal,
                      dobi.inma_symbol AS item_symbol,
                      dobi.inma_nazwa AS item_name,
                      dobi.ilosc AS quantity,
                      dobi.cena AS price,
                      dobi.wartosc AS VALUE
                 FROM ap_dokumenty_obrot_it dobi
                WHERE dobi.doob_id = doob.id
               ORDER BY dobi.numer)
           AS lines
  FROM ap_dokumenty_obrot doob
 WHERE     doob.wzty_kod = ''WZ''
       AND doob.numer_zamowienia IS NOT NULL
       AND doob.id IN (:p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <DELIVERY><xsl:apply-templates/></DELIVERY>
                     </xsl:template>
                     <xsl:template priority="2" match="LINES/LINES_ROW">
                        <LINE><xsl:apply-templates/></LINE>
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/deliveries',
                   'T',
                   'OUT');
END;
/
BEGIN
    lg_sql_wykonywanie.wykonaj_ddl (
        p_wyrazenie   => 'begin DBMS_SCHEDULER.DROP_JOB(job_name=> ''INTEGRACJAINFINITE''); end;',
        p_nr_bledu    => -27475);
    DBMS_SCHEDULER.create_job (
        '"INTEGRACJAINFINITE"',
        job_type              => 'PLSQL_BLOCK',
        job_action            => 'BEGIN jg_output_sync.PROCESS(); jg_input_sync.get_from_ftp(); END;',
        number_of_arguments   => 0,
        start_date            => TO_TIMESTAMP_TZ (
                                    '18-SEP-2016 12.40.32,357000000 PM +02:00',
                                    'DD-MON-RRRR HH.MI.SSXFF AM TZR',
                                    'NLS_DATE_LANGUAGE=english'),
        repeat_interval       => 'FREQ=MINUTELY; INTERVAL=10;',
        end_date              => NULL,
        job_class             => '"DEFAULT_JOB_CLASS"',
        enabled               => FALSE,
        auto_drop             => TRUE,
        comments              => NULL);
    DBMS_SCHEDULER.enable ('"INTEGRACJAINFINITE"');
    COMMIT;
END;
/


