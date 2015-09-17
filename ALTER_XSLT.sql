DECLARE
    v_xml     XMLTYPE;
BEGIN
    v_xml := XMLTYPE('<?xml version="1.0" encoding="WINDOWS-1250"?>
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
</xsl:stylesheet>');

    INSERT INTO jg_xslt_repository (ID, OBJECT_TYPE, XSLT)
         VALUES (JG_XSRE_SEQ.NEXTVAL, 'NEW_CUSTOMER', v_xml);

    v_xml := XMLTYPE('<?xml version="1.0" encoding="WINDOWS-1250"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml" encoding="windows-1250" indent="yes"/>
  <xsl:template match="/">
    <PA_KONTRAHENT_TK xmlns="http://www.teta.com.pl/teta2000/kontrahent-1" wersja="1.0">
      <xsl:for-each select="CustomerData">
        <xsl:for-each select="BasicData">
          <xsl:for-each select="ID">
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
</xsl:stylesheet>');

    INSERT INTO jg_xslt_repository (ID, OBJECT_TYPE, XSLT)
         VALUES (JG_XSRE_SEQ.NEXTVAL, 'CUSTOMER_DATA', v_xml);

    v_xml := XMLTYPE('<?xml version="1.0" encoding="WINDOWS-1250"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml" encoding="windows-1250" indent="yes"/>
  <xsl:template match="/">
    <LG_ZASP_T>
      <xsl:for-each select="ORDER">
        <WZORZEC>Zamówienie sprzedaży - Wrocław</WZORZEC>
        <TYP_ZAMOWIENIA>ZS</TYP_ZAMOWIENIA>
        <xsl:for-each select="ID">
          <SYMBOL_DOKUMENTU>
            <xsl:value-of select="."/>
          </SYMBOL_DOKUMENTU>
        </xsl:for-each>
        <xsl:for-each select="REALIZATION_DATE">
          <DATA_REALIZACJI>
            <xsl:value-of select="."/>
          </DATA_REALIZACJI>
        </xsl:for-each>
        <xsl:for-each select="REALIZATION_DATE">
          <DATA_WYSTAWIENIA>
            <xsl:value-of select="."/>
          </DATA_WYSTAWIENIA>
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
        <xsl:for-each select="NET_VALUE">
          <WARTOSC_NETTO>
            <xsl:value-of select="."/>
          </WARTOSC_NETTO>
        </xsl:for-each>
        <xsl:for-each select="PAYMENT_METHOD_ID">
          <KOD_FORMY_ZAPLATY>
            <xsl:value-of select="."/>
          </KOD_FORMY_ZAPLATY>
        </xsl:for-each>
        <xsl:for-each select="DELIVERY_METHOD_ID">
          <KOD_SPOSOBU_DOSTAWY>
            <xsl:value-of select="."/>
          </KOD_SPOSOBU_DOSTAWY>
        </xsl:for-each>
        <xsl:for-each select="PRICING_TYPE">
          <WG_JAKICH_CEN>
            <xsl:value-of select="."/>
          </WG_JAKICH_CEN>
        </xsl:for-each>
        <xsl:for-each select="DISCOUNT">
          <OPUST_GLOB_KWOTA>
            <xsl:value-of select="."/>
          </OPUST_GLOB_KWOTA>
        </xsl:for-each>
        <xsl:for-each select="PUSP_KOD">
          <KOD_PUNKTU_SPRZEDAZY>
            <xsl:value-of select="."/>
          </KOD_PUNKTU_SPRZEDAZY>
        </xsl:for-each>
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
        <ILOSC_DNI_DO_ZAPLATY>0</ILOSC_DNI_DO_ZAPLATY>
        <WSKAZNIK_ZATWIERDZENIA>T</WSKAZNIK_ZATWIERDZENIA>
        <POZYCJE>
          <xsl:for-each select="ITEMS">
            <xsl:for-each select="ITEMS_ROW">
              <LG_ZASI_T>
                <xsl:for-each select="ORDINAL_NUMBER">
                  <LP>
                    <xsl:value-of select="."/>
                  </LP>
                </xsl:for-each>
                <INDEKS>
                  <xsl:for-each select="COMMODITY_ID">
                    <INDEKS>
                      <xsl:value-of select="."/>
                    </INDEKS>
                  </xsl:for-each>
                  <xsl:for-each select="COMMODITY_NAME">
                    <NAZWA>
                      <xsl:value-of select="."/>
                    </NAZWA>
                  </xsl:for-each>
                </INDEKS>
                <xsl:for-each select="KOD_STAWKI_VAT">
                  <KOD_STAWKI_VAT>
                    <xsl:value-of select="."/>
                  </KOD_STAWKI_VAT>
                </xsl:for-each>
                <xsl:for-each select="QUANTITY_ORDERED">
                  <ILOSC>
                    <xsl:value-of select="."/>
                  </ILOSC>
                </xsl:for-each>
                <xsl:for-each select="CURRENCY">
                  <KOD_WALUTY>
                    <xsl:value-of select="."/>
                  </KOD_WALUTY>
                </xsl:for-each>
                <xsl:for-each select="NET_PRICE">
                  <CENA>
                    <xsl:value-of select="."/>
                  </CENA>
                </xsl:for-each>
                <xsl:for-each select="NET_PRICE">
                  <CENA_Z_CENNIKA>
                    <xsl:value-of select="."/>
                  </CENA_Z_CENNIKA>
                </xsl:for-each>
                <xsl:for-each select="NET_PRICE">
                  <CENA_Z_CENNIKA_WAL>
                    <xsl:value-of select="."/>
                  </CENA_Z_CENNIKA_WAL>
                </xsl:for-each>
                <xsl:for-each select="DISCOUNT">
                  <OPUST_NA_POZYCJI>
                    <xsl:value-of select="."/>
                  </OPUST_NA_POZYCJI>
                </xsl:for-each>
                <POLA_DODATKOWE>
                  <PA_POLE_DODATKOWE_T/>
                </POLA_DODATKOWE>
                <NAZWA_JEDNOSTKI_MIARY>szt.</NAZWA_JEDNOSTKI_MIARY>
              </LG_ZASI_T>
            </xsl:for-each>
          </xsl:for-each>
        </POZYCJE>
      </xsl:for-each>
    </LG_ZASP_T>
  </xsl:template>
</xsl:stylesheet>');

    INSERT INTO jg_xslt_repository (ID, OBJECT_TYPE, XSLT)
         VALUES (JG_XSRE_SEQ.NEXTVAL, 'ORDER', v_xml);
END;
/
