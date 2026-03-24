

print("Script gestart")


import requests
import pandas as pd
import numpy as np
import time
import yfinance as yf
import datetime as dt


file_path = "tickers_consumer_defensive.xlsx"

df = pd.read_excel(file_path)

tickers = df['Ticker']

###############################################################################
# Get SP500 data

ticker = yf.Ticker("^GSPC")

# Get prices
SP500_prices = ticker.history(start="2010-07-01", end=dt.datetime.today().strftime("%Y-%m-%d"))


###############################################################################
# Get yfinance data for stock


API_KEY_list = ['7RU1JBRFTG1YSQJF']

API_KEY_list = (API_KEY_list * 60)[:60]


data_list = []
empty_tickers_list = []
ticker_data_without_eps = []


for num in range(36, 42):

    API_KEY = API_KEY_list[num]
    SYMBOL = tickers[num]
    BASE_URL = "https://www.alphavantage.co/query"


    def call_alpha_vantage(function: str, symbol: str, **extra_params) -> dict:
        """
        Helper om een Alpha Vantage endpoint aan te roepen.
        """
        params = {
            "function": function,
            "symbol": symbol,
            "apikey": API_KEY,
        }
        params.update(extra_params)
        resp = requests.get(BASE_URL, params=params)
        resp.raise_for_status()
        data = resp.json()
        if "Information" in data or "Note" in data:
            print("Let op: mogelijk rate limit geraakt of foutmelding van API:")
            print(data)
        return data


    def get_income_statement(symbol: str):
        data = call_alpha_vantage("INCOME_STATEMENT", symbol)
        annual = pd.DataFrame(data.get("annualReports", []))
        quarterly = pd.DataFrame(data.get("quarterlyReports", []))

        # kolommen naar numeriek waar mogelijk
        for df in (annual, quarterly):
            for col in df.columns:
                if col != "fiscalDateEnding" and col != "reportedCurrency":
                    df[col] = pd.to_numeric(df[col], errors="coerce")

        return annual, quarterly


    def get_balance_sheet(symbol: str):
        data = call_alpha_vantage("BALANCE_SHEET", symbol)
        annual = pd.DataFrame(data.get("annualReports", []))
        quarterly = pd.DataFrame(data.get("quarterlyReports", []))

        for df in (annual, quarterly):
            for col in df.columns:
                if col != "fiscalDateEnding" and col != "reportedCurrency":
                    df[col] = pd.to_numeric(df[col], errors="coerce")

        return annual, quarterly


    def get_cash_flow(symbol: str):
        data = call_alpha_vantage("CASH_FLOW", symbol)
        annual = pd.DataFrame(data.get("annualReports", []))
        quarterly = pd.DataFrame(data.get("quarterlyReports", []))

        for df in (annual, quarterly):
            for col in df.columns:
                if col != "fiscalDateEnding" and col != "reportedCurrency":
                    df[col] = pd.to_numeric(df[col], errors="coerce")

        return annual, quarterly


    def get_earnings(symbol: str):
        """
        Haalt earnings / EPS data op:
        - annualEarnings
        - quarterlyEarnings
        Met o.a.:
          fiscalDateEnding, reportedDate, reportedEPS, estimatedEPS,
          surprise, surprisePercentage
        """
        data = call_alpha_vantage("EARNINGS", symbol)

        annual = pd.DataFrame(data.get("annualEarnings", []))
        quarterly = pd.DataFrame(data.get("quarterlyEarnings", []))

        # Datums netjes naar datetime
        for df in (annual, quarterly):
            if "fiscalDateEnding" in df.columns:
                df["fiscalDateEnding"] = pd.to_datetime(df["fiscalDateEnding"], errors="coerce")
            if "reportedDate" in df.columns:
                df["reportedDate"] = pd.to_datetime(df["reportedDate"], errors="coerce")

        # EPS en surprise naar numeriek
        for df in (annual, quarterly):
            for col in ("reportedEPS", "estimatedEPS", "surprise", "surprisePercentage"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

        return annual, quarterly



    if __name__ == "__main__":
        # 1) Income statement
        inc_annual, inc_quarterly = get_income_statement(SYMBOL)
        print("Income statement - annual:")
        print(inc_annual.head())
        print("\nIncome statement - quarterly:")
        print(inc_quarterly.head())
        time.sleep(1)

        # 2) Balance sheet
        bs_annual, bs_quarterly = get_balance_sheet(SYMBOL)
        print("\nBalance sheet - annual:")
        print(bs_annual.head())
        time.sleep(1)

        # 3) Cash flow
        cf_annual, cf_quarterly = get_cash_flow(SYMBOL)
        print("\nCash flow - annual:")
        print(cf_annual.head())
        time.sleep(1)

        # 4) Earnings / EPS
        eps_annual, eps_quarterly = get_earnings(SYMBOL)
        print("\nEarnings / EPS - annual:")
        print(eps_annual.head())
        print("\nEarnings / EPS - quarterly:")
        print(eps_quarterly.head())
        time.sleep(1)


    if not inc_quarterly.empty:
        if not eps_quarterly.empty:

            ###############################################################################
            # Get TTM data

            def compute_ttm_from_quarterly(
                df,
                date_col="fiscalDateEnding",
                currency_col=None,
                extra_meta_cols=None,
            ):
                """
                Neemt een quarterly DataFrame met meest recente periode op index 0
                en geeft een nieuw DataFrame terug met TTM (rolling 4 quarters) waarden
                voor alle numerieke kolommen.

                Parameters
                ----------
                df : pd.DataFrame
                    Quarterly data (meest recente observatie op index 0).
                date_col : str
                    Naam van de kolom met de periode-einddatum.
                currency_col : str or None
                    Optionele kolomnaam voor valuta (bv. 'reportedCurrency').
                    Als None of niet in df: wordt genegeerd.
                extra_meta_cols : list of str or None
                    Extra niet-numerieke kolommen die je wil behouden
                    (bv. ['reportedDate', 'reportTime']).
                """

                df = df.copy()

                # Datumkolom naar datetime
                df[date_col] = pd.to_datetime(df[date_col])

                # Bepaal meta-kolommen die we NIET willen sommeren
                meta_cols = [date_col]

                if currency_col is not None and currency_col in df.columns:
                    meta_cols.append(currency_col)

                if extra_meta_cols is not None:
                    for c in extra_meta_cols:
                        if c in df.columns and c not in meta_cols:
                            meta_cols.append(c)

                # Numerieke kolommen = rest
                num_cols = df.columns.difference(meta_cols)

                # Data staat nu: index 0 = meest recent
                # Voor rolling willen we chronologisch (oudste eerst), dus we draaien om
                df_rev = df.iloc[::-1].reset_index(drop=True)

                # Rolling-som over 4 kwartalen voor alle numerieke kolommen
                ttm_num = (
                    df_rev[num_cols]
                    .rolling(window=4, min_periods=4)  # pas vanaf 4 kwartalen krijg je een TTM
                    .sum()
                )

                # Meta-data: datum/currency/etc. van het einde van het venster
                ttm_meta = df_rev[meta_cols]

                # Combineer
                ttm_df_rev = pd.concat([ttm_meta, ttm_num], axis=1)

                # Draai terug zodat index 0 opnieuw de meest recente TTM is
                ttm_df = ttm_df_rev.iloc[::-1].reset_index(drop=True)

                # Eerste 3 rijen hebben geen volledige 4 kwartalen → droppen
                ttm_df = ttm_df.iloc[0:len(df) - 3].copy()

                return ttm_df

            def compute_4q_average_from_quarterly(df, date_col="fiscalDateEnding", currency_col="reportedCurrency"):
                """
                Neemt een quarterly balance sheet DataFrame (index 0 = meest recent),
                en geeft een DataFrame terug met 4-quarter averages voor alle numerieke velden.
                """
                df = df.copy()
                df[date_col] = pd.to_datetime(df[date_col])

                # Niet-numerieke kolommen scheiden
                non_num_cols = [date_col, currency_col]
                num_cols = df.columns.difference(non_num_cols)

                # Chronologisch maken (oudste eerst voor rolling)
                df_rev = df.iloc[::-1].reset_index(drop=True)

                # Rolling average over 4 kwartalen
                avg_num = (
                    df_rev[num_cols]
                    .rolling(window=4, min_periods=4)
                    .mean()
                )

                # Meta-data (datum/currency van "end" van venster)
                avg_meta = df_rev[[date_col, currency_col]]

                # Combineer
                avg_df_rev = pd.concat([avg_meta, avg_num], axis=1)

                # Terugdraaien zodat index 0 opnieuw meest recente periode is
                avg_df = avg_df_rev.iloc[::-1].reset_index(drop=True)

                # Eerste 3 rijen hebben geen volledige 4 kwartalen → droppen
                avg_df = avg_df.iloc[0:len(df) - 3].copy()

                return avg_df


            ttm_inc = compute_ttm_from_quarterly(inc_quarterly,
                                                 date_col="fiscalDateEnding",
                                                 currency_col="reportedCurrency")

            ttm_cf = compute_ttm_from_quarterly(cf_quarterly,
                                                date_col="fiscalDateEnding",
                                                currency_col="reportedCurrency")


            bs_avg = compute_4q_average_from_quarterly(bs_quarterly,
                                                       date_col="fiscalDateEnding",
                                                       currency_col="reportedCurrency")

            ttm_eps = compute_ttm_from_quarterly(
                eps_quarterly,
                date_col="fiscalDateEnding",
                currency_col=None,  # geen reportedCurrency in deze df
                extra_meta_cols=["reportedDate", "reportTime"],  # optioneel, als je die wil houden
            )

            ###############################################################################
            # Get variables



            def build_fundamental_ratios(ttm_inc, ttm_cf, bs_avg, bs_quarterly, inc_quarterly):
                """
                Maakt een DataFrame met zoveel mogelijk ratio's op basis van:
                  - ttm_inc: TTM income statement
                  - ttm_cf:  TTM cash flow
                  - bs_avg:  4Q average balance sheet
                  - bs_quarterly: point-in-time balance sheet
                  - inc_quarterly: kwartaal income statement (voor QoQ growth)
                """

                # ---------- 0) Zorg dat alle fiscalDateEnding kolommen datetime zijn ----------
                for df in [ttm_inc, ttm_cf, bs_avg, bs_quarterly, inc_quarterly]:
                    df["fiscalDateEnding"] = pd.to_datetime(df["fiscalDateEnding"])

                # 1) Kolommen van bs_avg en bs_quarterly suffixen (behalve keys)
                def rename_bs_cols(df, suffix):
                    return df.rename(columns=lambda c: c if c in ["fiscalDateEnding", "reportedCurrency"] else f"{c}{suffix}")

                bs_avg_ren = rename_bs_cols(bs_avg, "_avg")
                bs_q_ren   = rename_bs_cols(bs_quarterly, "_bsq")

                # 2) Merge TTM en balansdata op fiscalDateEnding + reportedCurrency
                df = (
                    ttm_inc.merge(ttm_cf, on=["fiscalDateEnding", "reportedCurrency"], how="inner", suffixes=("", "_cf"))
                          .merge(bs_avg_ren, on=["fiscalDateEnding", "reportedCurrency"], how="inner")
                          .merge(bs_q_ren, on=["fiscalDateEnding", "reportedCurrency"], how="inner")
                          .merge(inc_quarterly, on=["fiscalDateEnding", "reportedCurrency"], how="inner", suffixes=("", "_q"))
                )

                # Sorteer op datum oplopend voor alle growth berekeningen
                df = df.sort_values("fiscalDateEnding").reset_index(drop=True)

                # ---------- Basis TTM variabelen ----------
                net_income_ttm        = df["netIncome"]
                revenue_ttm           = df["totalRevenue"]
                gross_profit_ttm      = df["grossProfit"]
                income_before_tax_ttm = df["incomeBeforeTax"]
                ebit_ttm              = df["ebit"]
                ebitda_ttm            = df["ebitda"]
                tax_expense_ttm       = df["incomeTaxExpense"]
                rnd_ttm               = df["researchAndDevelopment"]

                # Cashflow / FCF
                operating_cf_ttm = df["operatingCashflow"]
                capex_ttm        = df["capitalExpenditures"]
                free_cf_ttm      = operating_cf_ttm - capex_ttm

                # Dividend TTM
                dividends_ttm = df.get("dividendPayout", pd.Series(index=df.index, dtype="float64"))

                # Gemiddelde balans (4Q avg)
                total_assets_avg = df["totalAssets_avg"]
                equity_avg       = df["totalShareholderEquity_avg"]
                liabilities_avg  = df["totalLiabilities_avg"]
                cash_avg         = df.get("cashAndCashEquivalentsAtCarryingValue_avg",
                                          df.get("cashAndShortTermInvestments_avg", pd.Series(0.0, index=df.index)))
                shareholderEquity = df['totalShareholderEquity_bsq']

                # Interest-bearing debt (gemiddeld)
                if "shortLongTermDebtTotal_avg" in df.columns:
                    debt_avg = df["shortLongTermDebtTotal_avg"]
                else:
                    debt_avg = (
                        df.get("longTermDebt_avg", 0).fillna(0) +
                        df.get("shortTermDebt_avg", 0).fillna(0)
                        #+
                        #df.get("currentDebt_avg", 0).fillna(0) +
                        #df.get("currentLongTermDebt_avg", 0).fillna(0)
                    )

                # Point-in-time balans (laatste kwartaal)
                current_assets_pt = df["totalCurrentAssets_bsq"]
                current_liab_pt   = df["totalCurrentLiabilities_bsq"]
                cash_pt           = df.get("cashAndCashEquivalentsAtCarryingValue_bsq",
                                           df.get("cashAndShortTermInvestments_bsq", pd.Series(0.0, index=df.index)))

                # Equity en debt op basis van laatste kwartaal (point-in-time)
                equity_lastq = df["totalShareholderEquity_bsq"]

                if "shortLongTermDebtTotal_bsq" in df.columns:
                    debt_lastq = df["shortLongTermDebtTotal_bsq"]
                else:
                    debt_lastq = (
                        df.get("longTermDebt_bsq", 0).fillna(0) +
                        df.get("shortTermDebt_bsq", 0).fillna(0)
                        #+
                        #df.get("currentDebt_bsq", 0).fillna(0) +
                        #df.get("currentLongTermDebt_bsq", 0).fillna(0)
                    )

                # Net debt (gemiddeld)
                net_debt_avg = debt_avg - cash_avg

                # Net debt (laatste kwartaal, point-in-time)
                net_debt_lastq = debt_lastq - cash_pt

                # Belastingvoet (TTM)
                tax_rate = tax_expense_ttm / income_before_tax_ttm.replace(0, np.nan)
                tax_rate = tax_rate.clip(lower=0, upper=1)  # sanity check

                # NOPAT en invested capital
                nopat_ttm            = ebit_ttm * (1 - tax_rate)
                invested_capital_avg = debt_avg + equity_avg - cash_avg

                # 3) Ratio's basisframe
                ratios = pd.DataFrame({
                    "fiscalDateEnding": df["fiscalDateEnding"],
                    "reportedCurrency": df["reportedCurrency"],
                })

                # ---------- Winstgevendheid ----------
                ratios["ROE"]          = net_income_ttm / equity_avg.replace(0, np.nan)
                ratios["ROA"]          = net_income_ttm / total_assets_avg.replace(0, np.nan)
                ratios["gross_margin"] = gross_profit_ttm / revenue_ttm.replace(0, np.nan)
                ratios["ebit_margin"]  = ebit_ttm / revenue_ttm.replace(0, np.nan)
                ratios["net_margin"]   = net_income_ttm / revenue_ttm.replace(0, np.nan)
                ratios["pretax_margin"] = income_before_tax_ttm / revenue_ttm.replace(0, np.nan)
                ratios["ROIC"]         = nopat_ttm / invested_capital_avg.replace(0, np.nan)
                ratios["capex_revenue_ttm"] = capex_ttm / revenue_ttm.replace(0, np.nan)

                # ---------- Cashflow / FCF ----------
                #ratios["free_cash_flow"]    = free_cf_ttm
                ratios["fcf_margin"]        = free_cf_ttm / revenue_ttm.replace(0, np.nan)
                ratios["fcf_to_net_income"] = free_cf_ttm / net_income_ttm.replace(0, np.nan)
                ratios["capex_operatingCF_ttm"] = capex_ttm / operating_cf_ttm.replace(0, np.nan)

                # ---------- Schuldratio's ----------
                ratios["debt_to_equity_avg"]   = debt_avg / equity_avg.replace(0, np.nan)
                ratios["debt_to_equity_lastq"] = debt_lastq / equity_lastq.replace(0, np.nan)

                interest_expense = df.get("interestExpense", df.get("interestAndDebtExpense", pd.Series(np.nan, index=df.index)))
                ratios["interest_coverage_ebit"] = ebit_ttm / interest_expense.replace(0, np.nan)
                ratios["net_debt"]               = net_debt_avg
                ratios["net_debt_to_fcf_avg"]    = net_debt_avg / free_cf_ttm.replace(0, np.nan)
                ratios["net_debt_to_fcf_lastq"]  = net_debt_lastq / free_cf_ttm.replace(0, np.nan)

                # ---------- Efficiëntie ----------
                ratios["asset_turnover"] = revenue_ttm / total_assets_avg.replace(0, np.nan)
                ratios['operatingCost/totalRevenue'] = df["operatingExpenses"] / revenue_ttm.replace(0, np.nan)

                # ---------- Liquiditeit ----------
                ratios["current_ratio"] = current_assets_pt / current_liab_pt.replace(0, np.nan)

                # ---------- Dividend ----------
                ratios["dividends_ttm"]         = dividends_ttm
                ratios["dividend_payout_ratio"] = dividends_ttm / net_income_ttm.replace(0, np.nan)

                # ---------- Groei (TTM, basis) ----------
                gross_profit_ttm_sorted = gross_profit_ttm.reset_index(drop=True)
                free_cf_ttm_sorted      = free_cf_ttm.reset_index(drop=True)
                dividends_ttm_sorted    = dividends_ttm.reset_index(drop=True)

                ratios["fcf_growth_log"] = np.log(free_cf_ttm_sorted.replace({0: np.nan})).diff()

                ratios["yoy_gross_profit_ttm_log"] = (
                    np.log(gross_profit_ttm_sorted.replace({0: np.nan})) -
                    np.log(gross_profit_ttm_sorted.replace({0: np.nan}).shift(4))
                )

                ratios["dividend_growth_log"] = np.log(dividends_ttm_sorted.replace({0: np.nan})).diff()

                # ---------- QoQ growth gross profit (quarterly) via inc_quarterly ----------
                inc_q = inc_quarterly.copy()
                inc_q = inc_q.sort_values("fiscalDateEnding").drop_duplicates("fiscalDateEnding", keep="last")

                inc_q_small = inc_q[["fiscalDateEnding", "grossProfit"]].rename(
                    columns={"grossProfit": "grossProfit_quarterly"}
                )

                ratios = ratios.merge(inc_q_small, on="fiscalDateEnding", how="left")

                gp_q = ratios["grossProfit_quarterly"]
                ratios["qoq_gross_profit_log"] = np.log(gp_q.replace({0: np.nan})).diff()

                # ---------- Research and development ----------
                ratios["rnd_intensity"]   = rnd_ttm / revenue_ttm.replace(0, np.nan)
                ratios["rnd_to_opex"]     = rnd_ttm / df["operatingExpenses"].replace(0, np.nan)
                ratios["rnd_efficiency"]  = gross_profit_ttm / rnd_ttm.replace(0, np.nan)
                ratios["rnd_to_assets"]   = rnd_ttm / total_assets_avg.replace(0, np.nan)
                ratios["rnd_shock"]       = df["researchAndDevelopment_q"] / rnd_ttm.replace(0, np.nan)

                # ---------- Long-term growth proxies ----------
                # Reinvestment rates
                ratios["reinvestment_rate_capex_only"] = capex_ttm / operating_cf_ttm.replace(0, np.nan)
                ratios["reinvestment_rate_capex_rnd"]  = (capex_ttm + rnd_ttm) / operating_cf_ttm.replace(0, np.nan)

                # Fundamentele LT growth proxy: ROIC × reinvestment rate
                ratios["lt_growth_roic_reinv"] = ratios["ROIC"] * ratios["reinvestment_rate_capex_rnd"]

                # YoY revenue growth (TTM) als extra LT-growth proxy
                revenue_ttm_sorted      = revenue_ttm.reset_index(drop=True)
                net_income_ttm_sorted   = net_income_ttm.reset_index(drop=True)

                ratios["yoy_revenue_ttm_log"] = (
                    np.log(revenue_ttm_sorted.replace({0: np.nan})) -
                    np.log(revenue_ttm_sorted.replace({0: np.nan}).shift(4))
                )

                ratios["yoy_net_income_ttm_log"] = (
                    np.log(net_income_ttm_sorted.replace({0: np.nan})) -
                    np.log(net_income_ttm_sorted.replace({0: np.nan}).shift(4))
                )

                # ---------- Data voor valuation/prijs-gerelateerde ratios ----------
                ratios["commonStockSharesOutstanding_bsq"] = df.loc[ratios.index, "commonStockSharesOutstanding_bsq"]
                ratios["netDebtLastq"]   = net_debt_lastq
                ratios["netDebtAvg"]     = net_debt_avg
                ratios["net_income_ttm"] = net_income_ttm
                ratios["revenue_ttm"]    = revenue_ttm
                ratios["free_cf_ttm"]    = free_cf_ttm
                ratios["ebitda_ttm"]     = ebitda_ttm
                ratios["operating_cf_ttm"] = operating_cf_ttm
                ratios["dividends_ttm"]  = dividends_ttm_sorted.loc[ratios.index]
                ratios["capex"]          = capex_ttm
                ratios["totalShareholderEquity"] = shareholderEquity
                ratios["grossProfit"] = gross_profit_ttm
                ratios["ebit_ttm"]          = ebit_ttm
                ratios["rnd"]          = rnd_ttm



                # Laatste stap: eventueel nieuwste periode bovenaan
                # ratios = ratios.sort_values("fiscalDateEnding", ascending=False).reset_index(drop=True)

                return ratios



            # Get dataframe with ratios
            ratios_df = build_fundamental_ratios(ttm_inc, ttm_cf, bs_avg, bs_quarterly, inc_quarterly)

            # Sorteer op datum oplopend voor alle growth berekeningen
            ttm_eps = ttm_eps.sort_values("fiscalDateEnding").reset_index(drop=True)

            # Get eps data
            ratios_df = ratios_df.merge(
                ttm_eps,
                on="fiscalDateEnding",
                how="inner",
                suffixes=("", "_ttmcf")
            )

            ratios_df = ratios_df.merge(
                eps_quarterly[['surprisePercentage', 'fiscalDateEnding']],
                on="fiscalDateEnding",
                how="inner",
                suffixes=("", "_perQuarter")
            )



            # Lag estimated EPS
            ratios_df['estimatedEPS'] = ratios_df['estimatedEPS'].shift(-1)



            ###############################################################################
            # Get data yahoo finance

            # Get ticker data
            ticker = yf.Ticker(SYMBOL)

            # Get prices
            prices = ticker.history(start="2010-07-01", end=dt.datetime.today().strftime("%Y-%m-%d"))

            # Get general information
            info = ticker.info

            # Add industry
            ratios_df['industry'] = info['industry']



            # deze herbekijken? lijkt nogal groot verschil

            # waar estimated EPS is nan => forward EPS yfinance
            ratios_df['estimatedEPS'] = ratios_df['estimatedEPS'].fillna(info['epsForward'])





            ###############################################################################
            # lag variables QoQ and YoY using relative percentage differences

            cols = ratios_df.columns.tolist()

            to_remove = ['industry', 'fiscalDateEnding', 'reportedCurrency', 'net_debt', 'fcf_growth_log',
                         'yoy_gross_profit_ttm_log', 'dividend_growth_log', 'qoq_gross_profit_log',
                         'yoy_revenue_ttm_log', 'yoy_net_income_ttm_log', 'commonStockSharesOutstanding_bsq',
                         'netDebtLastq', 'netDebtAvg', 'reportedDate', 'reportTime', 'estimatedEPS',
                         'surprise', 'surprisePercentage', 'surprisePercentage_perQuarter']  # columns you want to delete from the list

            cols = [c for c in cols if c not in to_remove]

            # QoQ
            for v in cols:
                ratios_df[f'{v}_QoQ'] = ratios_df[f'{v}'].pct_change()

            # YoY
            for v in cols:
                ratios_df[f'{v}_YoY'] = ratios_df[f'{v}'].pct_change(periods=4)

            # Changes for estimated eps and actual eps
            ratios_df['estimated_vs_current_eps'] = (ratios_df['estimatedEPS'] - ratios_df['reportedEPS']) / abs(ratios_df['reportedEPS'])


            # Hanlde changes with crossings



            ###############################################################################
            # Merge price data with ratios_df
            ratios_df['reportedDate'] = pd.to_datetime(ratios_df['reportedDate'], errors='coerce')
            prices['priceDate'] = pd.to_datetime(prices.index, errors='coerce')

            # timezone verwijderen indien aanwezig
            if getattr(prices['priceDate'].dt, 'tz', None) is not None:
                prices['priceDate'] = prices['priceDate'].dt.tz_localize(None)

            if getattr(ratios_df['reportedDate'].dt, 'tz', None) is not None:
                ratios_df['reportedDate'] = ratios_df['reportedDate'].dt.tz_localize(None)

            # forceer exact hetzelfde datetime-type
            ratios_df['reportedDate'] = ratios_df['reportedDate'].astype('datetime64[ns]')
            prices['priceDate'] = prices['priceDate'].astype('datetime64[ns]')

            # verwijder lege datums
            ratios_df = ratios_df.dropna(subset=['reportedDate'])
            prices = prices.dropna(subset=['priceDate'])

            ratios_df = ratios_df.sort_values('reportedDate')
            prices = prices.sort_values('priceDate')

            merged_df = pd.merge_asof(
                prices,
                ratios_df,
                left_on='priceDate',
                right_on='reportedDate',
                direction='backward'
            )

            

            merged_df = merged_df.set_index('priceDate')

            # extra variabelen: aantal dagen tot volgende earnings release
            merged_df['daysSinceRelease'] = (
                merged_df.index - merged_df['reportedDate']
            ).dt.days



            # Get ratios with market cap
            merged_df['marketCap'] = merged_df['Close'] * merged_df['commonStockSharesOutstanding_bsq']

            merged_df['net_income_ttm/marketCap'] = merged_df['net_income_ttm'] / merged_df['marketCap']
            merged_df['revenue_ttm/marketCap'] = merged_df['revenue_ttm'] / merged_df['marketCap']
            merged_df['estimatedEPS/Close'] = merged_df['estimatedEPS'] / merged_df['Close']
            merged_df['free_cf_ttm/marketCap'] = merged_df['free_cf_ttm'] / merged_df['marketCap']
            merged_df['operating_cf_ttm/marketCap'] = merged_df['operating_cf_ttm'] / merged_df['marketCap']
            merged_df['reportedEPS/marketCap'] = merged_df['reportedEPS'] / merged_df['marketCap']
            merged_df['estimatedEPS/marketCap'] = merged_df['estimatedEPS'] / merged_df['marketCap']
            merged_df['dividends_ttm/marketCap'] = merged_df['dividends_ttm'] / merged_df['marketCap']
            merged_df['bookValue/marketCap'] = merged_df['totalShareholderEquity'] / merged_df['marketCap']


            # Get ratios with enterprise value
            merged_df['enterprisevalue'] = merged_df['marketCap'] + merged_df['netDebtLastq']

            merged_df['free_cf_ttm/enterprisevalue'] = merged_df['free_cf_ttm'] / merged_df['enterprisevalue']
            merged_df['operating_cf_ttm/enterprisevalue'] = merged_df['operating_cf_ttm'] / merged_df['enterprisevalue']
            merged_df['ebitda_ttm/enterprisevalue'] = merged_df['ebitda_ttm'] / merged_df['enterprisevalue']
            merged_df['ebit_ttm/enterprisevalue'] = merged_df['ebit_ttm'] / merged_df['enterprisevalue']
            merged_df['capex/enterprisevalue'] = merged_df['capex'] / merged_df['enterprisevalue']
            merged_df['revenue_ttm/enterprisevalue'] = merged_df['revenue_ttm'] / merged_df['enterprisevalue']


            cols = ['net_income_ttm/marketCap', 'revenue_ttm/marketCap', 'free_cf_ttm/marketCap', 'operating_cf_ttm/marketCap',
                    'reportedEPS/marketCap', 'estimatedEPS/marketCap', 'dividends_ttm/marketCap',
                    'bookValue/marketCap', 'enterprisevalue', 'free_cf_ttm/enterprisevalue',
                    'operating_cf_ttm/enterprisevalue', 'ebitda_ttm/enterprisevalue', 'ebit_ttm/enterprisevalue',
                    'capex/enterprisevalue', 'revenue_ttm/enterprisevalue', 'estimatedEPS/Close']

            # deze moeten nog gelagged worden??
            for v in cols:
                merged_df[f'{v}_DoD'] = merged_df[f'{v}'].pct_change()


            ###############################################################################
            # Get rolling beta

            merged_df.index = pd.to_datetime(merged_df.index).tz_localize(None)
            SP500_prices.index = pd.to_datetime(SP500_prices.index).tz_localize(None)

            # --- 3) Compute daily returns ---
            stock_ret = merged_df['Close'].pct_change().rename("stock_ret")
            mkt_ret   = SP500_prices['Close'].pct_change().rename("mkt_ret")

            # --- 4) Align & drop missing dates ---
            ret_df = pd.concat([stock_ret, mkt_ret], axis=1).dropna()

            # --- 5) Rolling beta ---
            window = 60  # e.g. 60 trading days; change to 126/252 if you want
            beta = (
                ret_df["stock_ret"].rolling(window).cov(ret_df["mkt_ret"])
                / ret_df["mkt_ret"].rolling(window).var()
            )

            # --- 6) Add back to merged_df ---
            merged_df[f"beta_{window}d"] = beta.reindex(merged_df.index)

            # --- 5) Rolling beta ---
            window = 120  # e.g. 60 trading days; change to 126/252 if you want
            beta = (
                ret_df["stock_ret"].rolling(window).cov(ret_df["mkt_ret"])
                / ret_df["mkt_ret"].rolling(window).var()
            )

            # --- 6) Add back to merged_df ---
            merged_df[f"beta_{window}d"] = beta.reindex(merged_df.index)

            # --- 5) Rolling beta ---
            window = 240  # e.g. 60 trading days; change to 126/252 if you want
            beta = (
                ret_df["stock_ret"].rolling(window).cov(ret_df["mkt_ret"])
                / ret_df["mkt_ret"].rolling(window).var()
            )

            # --- 6) Add back to merged_df ---
            merged_df[f"beta_{window}d"] = beta.reindex(merged_df.index)

            # Add beta spread
            merged_df["beta_spread_60_240"] = merged_df["beta_60d"] - merged_df["beta_240d"]


            ###############################################################################
            # Technical indicators
            ###############################################################################
            # momentum

            px = merged_df['Close']

            # trading-day approximations
            d1, d3, d6 = 21, 63, 126

            # 3-1M momentum: return from (t-3M) -> (t-1M)
            merged_df["mom_3_1M"] = px.shift(d1) / px.shift(d3) - 1

            # 6-1M momentum: return from (t-6M) -> (t-1M)
            merged_df["mom_6_1M"] = px.shift(d1) / px.shift(d6) - 1

            # 1-3M momentum (common definition): short-term vs medium-term momentum
            # = 1M return minus 3M return (often used as a reversal / acceleration signal)
            ret_1M = px / px.shift(d1) - 1
            ret_3M = px / px.shift(d3) - 1
            merged_df["mom_1_3M"] = ret_1M - ret_3M


            # --- Reversal indicators ---
            # (often defined as the negative of recent returns: strong recent gain -> negative reversal signal)
            merged_df["rev_1w"] = -(px.pct_change(5))     # 1-week reversal
            merged_df["rev_1m"] = -(px.pct_change(21))    # 1-month reversal

            # --- 52-week high and distance to it ---
            high_52w = px.rolling(252, min_periods=252).max()
            #merged_df["high_52w"] = high_52w

            # Distance to 52W high: (Price / High) - 1  -> 0 at the high, negative below the high
            merged_df["dist_to_52w_high"] = (px / high_52w) - 1

            ###############################################################################
            # trend

            # pick price column
            price_col = "close" if "close" in merged_df.columns else "Close"
            px = merged_df[price_col].astype(float)

            # --- Moving averages ---
            ma_50  = px.rolling(50, min_periods=50).mean()
            ma_200 = px.rolling(200, min_periods=200).mean()

            # --- Moving average trend features ---
            # Price vs MA200 (percentage distance)
            merged_df["px_vs_ma200"] = (px / ma_200) - 1

            # MA50 - MA200 spread (percentage; scale-free)
            merged_df["ma50_ma200_spread"] = (ma_50 / ma_200) - 1

            # --- Trend strength: slope of log price over 63d ---
            # slope in "log-price units per day" (bigger = stronger uptrend)
            log_px = np.log(px)

            def slope(x: np.ndarray) -> float:
                x = np.asarray(x, dtype="float64")
                if np.any(np.isnan(x)):
                    return np.nan
                t = np.arange(len(x), dtype="float64")
                # OLS slope = cov(t,x)/var(t)
                return np.cov(t, x, ddof=0)[0, 1] / np.var(t)

            merged_df["logpx_slope_63d"] = log_px.rolling(63, min_periods=63).apply(slope, raw=True)



            ###############################################################################
            # volatility

            # daily returns (log returns are common for vol)
            ret = np.log(px).diff()

            ann = np.sqrt(252)

            # --- Realised volatility (annualised) ---
            merged_df["rv_20d"] = ret.rolling(20, min_periods=20).std() * ann
            merged_df["rv_60d"] = ret.rolling(60, min_periods=60).std() * ann

            # --- Vol-of-vol (regime / instability indicator) ---
            # rolling std of realised vol itself
            merged_df["vov_20d"] = merged_df["rv_20d"].rolling(20, min_periods=20).std()
            merged_df["vov_60d"] = merged_df["rv_60d"].rolling(60, min_periods=60).std()

            # Optional: relative vol-of-vol (scale-free)
            merged_df["vov_20d_rel"] = merged_df["vov_20d"] / merged_df["rv_20d"]
            merged_df["vov_60d_rel"] = merged_df["vov_60d"] / merged_df["rv_60d"]

            ###############################################################################
            # Drawdown

            # --- auto-detect column names ---
            close_col = "close" if "close" in merged_df.columns else "Close"
            high_col  = "high"  if "high"  in merged_df.columns else "High"
            low_col   = "low"   if "low"   in merged_df.columns else "Low"

            px = merged_df[close_col]

            # --- helper: max drawdown inside a window (returns a negative number, e.g., -0.27 = -27%) ---
            def max_drawdown_window(arr: np.ndarray) -> float:
                arr = np.asarray(arr, dtype="float64")
                if np.all(np.isnan(arr)):
                    return np.nan
                peak = np.maximum.accumulate(arr)
                dd = arr / peak - 1.0
                return np.nanmin(dd)

            # 3M and 6M rolling windows (trading-day approximations)
            w3m, w6m = 63, 126

            merged_df["mdd_3m"] = px.rolling(w3m, min_periods=w3m).apply(max_drawdown_window, raw=True)
            merged_df["mdd_6m"] = px.rolling(w6m, min_periods=w6m).apply(max_drawdown_window, raw=True)

            # --- ATR(14) ---
            # True Range (TR)
            prev_close = merged_df[close_col].shift(1)
            tr = pd.concat([
                (merged_df[high_col] - merged_df[low_col]).abs(),
                (merged_df[high_col] - prev_close).abs(),
                (merged_df[low_col] - prev_close).abs()
            ], axis=1).max(axis=1)

            # ATR(14): Wilder's smoothing (common)
            atr_14 = tr.ewm(alpha=1/14, adjust=False, min_periods=14).mean()

            # Normalized ATR: ATR / price
            merged_df["atr14_norm"] = atr_14 / merged_df[close_col]


            ###############################################################################
            # Volume, liquidity and attention

            # ---- column detection ----
            price_col = "close" if "close" in merged_df.columns else "Close"
            vol_col   = "volume" if "volume" in merged_df.columns else "Volume"

            px = merged_df[price_col].astype(float)
            vol = merged_df[vol_col].astype(float)

            # ---- Dollar Volume ----
            dollar_volume = px * vol

            # 20d average dollar volume
            merged_df["dollar_volume_20d_avg"] = dollar_volume.rolling(20, min_periods=20).mean()

            # Dollar volume trend (2 opties; kies wat je prefereert)
            # Optie A (simpel & interpreteerbaar): log-ratio vs 60d baseline
            dollar_volume_60d_avg = dollar_volume.rolling(60, min_periods=60).mean()
            merged_df["dollar_volume_trend"] = np.log(
                (merged_df["dollar_volume_20d_avg"] / dollar_volume_60d_avg).replace(0, np.nan)
            )

            # ---- Volume surprise / attention: z-score (20d) ----
            vol_mean_20 = vol.rolling(20, min_periods=20).mean()
            vol_std_20  = vol.rolling(20, min_periods=20).std()

            merged_df["volume_z_20d"] = (vol - vol_mean_20) / vol_std_20.replace(0, np.nan)

            # ---- Abnormal dollar value (P x V): z-score (20d) ----
            dv = dollar_volume
            dv_mean_20 = dv.rolling(20, min_periods=20).mean()
            dv_std_20  = dv.rolling(20, min_periods=20).std()

            merged_df["dollar_volume_z_20d"] = (dv - dv_mean_20) / dv_std_20.replace(0, np.nan)

            # ---- Amihud illiquidity (20d) ----
            # Amihud ~ avg_t( |return_t| / dollar_volume_t )
            ret = px.pct_change()
            amihud_daily = ret.abs() / dv.replace(0, np.nan)

            merged_df["amihud_20d"] = amihud_daily.rolling(20, min_periods=20).mean()



            ###############################################################################
            # price lags -1 day, -5, -10,...

            merged_df['price_lag_1'] = merged_df['Close'].pct_change(periods=1)
            merged_df['price_lag_5'] = merged_df['Close'].pct_change(periods=5)
            merged_df['price_lag_20'] = merged_df['Close'].pct_change(periods=20)
            merged_df['price_lag_60'] = merged_df['Close'].pct_change(periods=60)


            ###############################################################################
            # Target variables
            ###############################################################################
            # +20, +40, +60, +120

            # Targets: future returns over +20, +40, +60, +120 trading days
            merged_df['return_20']  = merged_df['Close'].shift(-20)  / merged_df['Close'] - 1
            merged_df['return_40']  = merged_df['Close'].shift(-40)  / merged_df['Close'] - 1
            merged_df['return_60']  = merged_df['Close'].shift(-60)  / merged_df['Close'] - 1
            merged_df['return_120'] = merged_df['Close'].shift(-120) / merged_df['Close'] - 1


            merged_df['Ticker'] = SYMBOL

            data_list.append(merged_df)

            time.sleep(30)



        else:

            ###############################################################################
            # Get TTM data

            def compute_ttm_from_quarterly(
                df,
                date_col="fiscalDateEnding",
                currency_col=None,
                extra_meta_cols=None,
            ):
                """
                Neemt een quarterly DataFrame met meest recente periode op index 0
                en geeft een nieuw DataFrame terug met TTM (rolling 4 quarters) waarden
                voor alle numerieke kolommen.

                Parameters
                ----------
                df : pd.DataFrame
                    Quarterly data (meest recente observatie op index 0).
                date_col : str
                    Naam van de kolom met de periode-einddatum.
                currency_col : str or None
                    Optionele kolomnaam voor valuta (bv. 'reportedCurrency').
                    Als None of niet in df: wordt genegeerd.
                extra_meta_cols : list of str or None
                    Extra niet-numerieke kolommen die je wil behouden
                    (bv. ['reportedDate', 'reportTime']).
                """

                df = df.copy()

                # Datumkolom naar datetime
                df[date_col] = pd.to_datetime(df[date_col])

                # Bepaal meta-kolommen die we NIET willen sommeren
                meta_cols = [date_col]

                if currency_col is not None and currency_col in df.columns:
                    meta_cols.append(currency_col)

                if extra_meta_cols is not None:
                    for c in extra_meta_cols:
                        if c in df.columns and c not in meta_cols:
                            meta_cols.append(c)

                # Numerieke kolommen = rest
                num_cols = df.columns.difference(meta_cols)

                # Data staat nu: index 0 = meest recent
                # Voor rolling willen we chronologisch (oudste eerst), dus we draaien om
                df_rev = df.iloc[::-1].reset_index(drop=True)

                # Rolling-som over 4 kwartalen voor alle numerieke kolommen
                ttm_num = (
                    df_rev[num_cols]
                    .rolling(window=4, min_periods=4)  # pas vanaf 4 kwartalen krijg je een TTM
                    .sum()
                )

                # Meta-data: datum/currency/etc. van het einde van het venster
                ttm_meta = df_rev[meta_cols]

                # Combineer
                ttm_df_rev = pd.concat([ttm_meta, ttm_num], axis=1)

                # Draai terug zodat index 0 opnieuw de meest recente TTM is
                ttm_df = ttm_df_rev.iloc[::-1].reset_index(drop=True)

                # Eerste 3 rijen hebben geen volledige 4 kwartalen → droppen
                ttm_df = ttm_df.iloc[0:len(df) - 3].copy()

                return ttm_df

            def compute_4q_average_from_quarterly(df, date_col="fiscalDateEnding", currency_col="reportedCurrency"):
                """
                Neemt een quarterly balance sheet DataFrame (index 0 = meest recent),
                en geeft een DataFrame terug met 4-quarter averages voor alle numerieke velden.
                """
                df = df.copy()
                df[date_col] = pd.to_datetime(df[date_col])

                # Niet-numerieke kolommen scheiden
                non_num_cols = [date_col, currency_col]
                num_cols = df.columns.difference(non_num_cols)

                # Chronologisch maken (oudste eerst voor rolling)
                df_rev = df.iloc[::-1].reset_index(drop=True)

                # Rolling average over 4 kwartalen
                avg_num = (
                    df_rev[num_cols]
                    .rolling(window=4, min_periods=4)
                    .mean()
                )

                # Meta-data (datum/currency van "end" van venster)
                avg_meta = df_rev[[date_col, currency_col]]

                # Combineer
                avg_df_rev = pd.concat([avg_meta, avg_num], axis=1)

                # Terugdraaien zodat index 0 opnieuw meest recente periode is
                avg_df = avg_df_rev.iloc[::-1].reset_index(drop=True)

                # Eerste 3 rijen hebben geen volledige 4 kwartalen → droppen
                avg_df = avg_df.iloc[0:len(df) - 3].copy()

                return avg_df


            ttm_inc = compute_ttm_from_quarterly(inc_quarterly,
                                                 date_col="fiscalDateEnding",
                                                 currency_col="reportedCurrency")

            ttm_cf = compute_ttm_from_quarterly(cf_quarterly,
                                                date_col="fiscalDateEnding",
                                                currency_col="reportedCurrency")


            bs_avg = compute_4q_average_from_quarterly(bs_quarterly,
                                                       date_col="fiscalDateEnding",
                                                       currency_col="reportedCurrency")

            # ttm_eps = compute_ttm_from_quarterly(
            #     eps_quarterly,
            #     date_col="fiscalDateEnding",
            #     currency_col=None,  # geen reportedCurrency in deze df
            #     extra_meta_cols=["reportedDate", "reportTime"],  # optioneel, als je die wil houden
            # )

            ###############################################################################
            # Get variables



            def build_fundamental_ratios(ttm_inc, ttm_cf, bs_avg, bs_quarterly, inc_quarterly):
                """
                Maakt een DataFrame met zoveel mogelijk ratio's op basis van:
                  - ttm_inc: TTM income statement
                  - ttm_cf:  TTM cash flow
                  - bs_avg:  4Q average balance sheet
                  - bs_quarterly: point-in-time balance sheet
                  - inc_quarterly: kwartaal income statement (voor QoQ growth)
                """

                # ---------- 0) Zorg dat alle fiscalDateEnding kolommen datetime zijn ----------
                for df in [ttm_inc, ttm_cf, bs_avg, bs_quarterly, inc_quarterly]:
                    df["fiscalDateEnding"] = pd.to_datetime(df["fiscalDateEnding"])

                # 1) Kolommen van bs_avg en bs_quarterly suffixen (behalve keys)
                def rename_bs_cols(df, suffix):
                    return df.rename(columns=lambda c: c if c in ["fiscalDateEnding", "reportedCurrency"] else f"{c}{suffix}")

                bs_avg_ren = rename_bs_cols(bs_avg, "_avg")
                bs_q_ren   = rename_bs_cols(bs_quarterly, "_bsq")

                # 2) Merge TTM en balansdata op fiscalDateEnding + reportedCurrency
                df = (
                    ttm_inc.merge(ttm_cf, on=["fiscalDateEnding", "reportedCurrency"], how="inner", suffixes=("", "_cf"))
                          .merge(bs_avg_ren, on=["fiscalDateEnding", "reportedCurrency"], how="inner")
                          .merge(bs_q_ren, on=["fiscalDateEnding", "reportedCurrency"], how="inner")
                          .merge(inc_quarterly, on=["fiscalDateEnding", "reportedCurrency"], how="inner", suffixes=("", "_q"))
                )

                # Sorteer op datum oplopend voor alle growth berekeningen
                df = df.sort_values("fiscalDateEnding").reset_index(drop=True)

                # ---------- Basis TTM variabelen ----------
                net_income_ttm        = df["netIncome"]
                revenue_ttm           = df["totalRevenue"]
                gross_profit_ttm      = df["grossProfit"]
                income_before_tax_ttm = df["incomeBeforeTax"]
                ebit_ttm              = df["ebit"]
                ebitda_ttm            = df["ebitda"]
                tax_expense_ttm       = df["incomeTaxExpense"]
                rnd_ttm               = df["researchAndDevelopment"]

                # Cashflow / FCF
                operating_cf_ttm = df["operatingCashflow"]
                capex_ttm        = df["capitalExpenditures"]
                free_cf_ttm      = operating_cf_ttm - capex_ttm

                # Dividend TTM
                dividends_ttm = df.get("dividendPayout", pd.Series(index=df.index, dtype="float64"))

                # Gemiddelde balans (4Q avg)
                total_assets_avg = df["totalAssets_avg"]
                equity_avg       = df["totalShareholderEquity_avg"]
                liabilities_avg  = df["totalLiabilities_avg"]
                cash_avg         = df.get("cashAndCashEquivalentsAtCarryingValue_avg",
                                          df.get("cashAndShortTermInvestments_avg", pd.Series(0.0, index=df.index)))
                shareholderEquity = df['totalShareholderEquity_bsq']

                # Interest-bearing debt (gemiddeld)
                if "shortLongTermDebtTotal_avg" in df.columns:
                    debt_avg = df["shortLongTermDebtTotal_avg"]
                else:
                    debt_avg = (
                        df.get("longTermDebt_avg", 0).fillna(0) +
                        df.get("shortTermDebt_avg", 0).fillna(0)
                        #+
                        #df.get("currentDebt_avg", 0).fillna(0) +
                        #df.get("currentLongTermDebt_avg", 0).fillna(0)
                    )

                # Point-in-time balans (laatste kwartaal)
                current_assets_pt = df["totalCurrentAssets_bsq"]
                current_liab_pt   = df["totalCurrentLiabilities_bsq"]
                cash_pt           = df.get("cashAndCashEquivalentsAtCarryingValue_bsq",
                                           df.get("cashAndShortTermInvestments_bsq", pd.Series(0.0, index=df.index)))

                # Equity en debt op basis van laatste kwartaal (point-in-time)
                equity_lastq = df["totalShareholderEquity_bsq"]

                if "shortLongTermDebtTotal_bsq" in df.columns:
                    debt_lastq = df["shortLongTermDebtTotal_bsq"]
                else:
                    debt_lastq = (
                        df.get("longTermDebt_bsq", 0).fillna(0) +
                        df.get("shortTermDebt_bsq", 0).fillna(0)
                        #+
                        #df.get("currentDebt_bsq", 0).fillna(0) +
                        #df.get("currentLongTermDebt_bsq", 0).fillna(0)
                    )

                # Net debt (gemiddeld)
                net_debt_avg = debt_avg - cash_avg

                # Net debt (laatste kwartaal, point-in-time)
                net_debt_lastq = debt_lastq - cash_pt

                # Belastingvoet (TTM)
                tax_rate = tax_expense_ttm / income_before_tax_ttm.replace(0, np.nan)
                tax_rate = tax_rate.clip(lower=0, upper=1)  # sanity check

                # NOPAT en invested capital
                nopat_ttm            = ebit_ttm * (1 - tax_rate)
                invested_capital_avg = debt_avg + equity_avg - cash_avg

                # 3) Ratio's basisframe
                ratios = pd.DataFrame({
                    "fiscalDateEnding": df["fiscalDateEnding"],
                    "reportedCurrency": df["reportedCurrency"],
                })

                # ---------- Winstgevendheid ----------
                ratios["ROE"]          = net_income_ttm / equity_avg.replace(0, np.nan)
                ratios["ROA"]          = net_income_ttm / total_assets_avg.replace(0, np.nan)
                ratios["gross_margin"] = gross_profit_ttm / revenue_ttm.replace(0, np.nan)
                ratios["ebit_margin"]  = ebit_ttm / revenue_ttm.replace(0, np.nan)
                ratios["net_margin"]   = net_income_ttm / revenue_ttm.replace(0, np.nan)
                ratios["pretax_margin"] = income_before_tax_ttm / revenue_ttm.replace(0, np.nan)
                ratios["ROIC"]         = nopat_ttm / invested_capital_avg.replace(0, np.nan)
                ratios["capex_revenue_ttm"] = capex_ttm / revenue_ttm.replace(0, np.nan)

                # ---------- Cashflow / FCF ----------
                #ratios["free_cash_flow"]    = free_cf_ttm
                ratios["fcf_margin"]        = free_cf_ttm / revenue_ttm.replace(0, np.nan)
                ratios["fcf_to_net_income"] = free_cf_ttm / net_income_ttm.replace(0, np.nan)
                ratios["capex_operatingCF_ttm"] = capex_ttm / operating_cf_ttm.replace(0, np.nan)

                # ---------- Schuldratio's ----------
                ratios["debt_to_equity_avg"]   = debt_avg / equity_avg.replace(0, np.nan)
                ratios["debt_to_equity_lastq"] = debt_lastq / equity_lastq.replace(0, np.nan)

                interest_expense = df.get("interestExpense", df.get("interestAndDebtExpense", pd.Series(np.nan, index=df.index)))
                ratios["interest_coverage_ebit"] = ebit_ttm / interest_expense.replace(0, np.nan)
                ratios["net_debt"]               = net_debt_avg
                ratios["net_debt_to_fcf_avg"]    = net_debt_avg / free_cf_ttm.replace(0, np.nan)
                ratios["net_debt_to_fcf_lastq"]  = net_debt_lastq / free_cf_ttm.replace(0, np.nan)

                # ---------- Efficiëntie ----------
                ratios["asset_turnover"] = revenue_ttm / total_assets_avg.replace(0, np.nan)
                ratios['operatingCost/totalRevenue'] = df["operatingExpenses"] / revenue_ttm.replace(0, np.nan)

                # ---------- Liquiditeit ----------
                ratios["current_ratio"] = current_assets_pt / current_liab_pt.replace(0, np.nan)

                # ---------- Dividend ----------
                ratios["dividends_ttm"]         = dividends_ttm
                ratios["dividend_payout_ratio"] = dividends_ttm / net_income_ttm.replace(0, np.nan)

                # ---------- Groei (TTM, basis) ----------
                gross_profit_ttm_sorted = gross_profit_ttm.reset_index(drop=True)
                free_cf_ttm_sorted      = free_cf_ttm.reset_index(drop=True)
                dividends_ttm_sorted    = dividends_ttm.reset_index(drop=True)

                ratios["fcf_growth_log"] = np.log(free_cf_ttm_sorted.replace({0: np.nan})).diff()

                ratios["yoy_gross_profit_ttm_log"] = (
                    np.log(gross_profit_ttm_sorted.replace({0: np.nan})) -
                    np.log(gross_profit_ttm_sorted.replace({0: np.nan}).shift(4))
                )

                ratios["dividend_growth_log"] = np.log(dividends_ttm_sorted.replace({0: np.nan})).diff()

                # ---------- QoQ growth gross profit (quarterly) via inc_quarterly ----------
                inc_q = inc_quarterly.copy()
                inc_q = inc_q.sort_values("fiscalDateEnding").drop_duplicates("fiscalDateEnding", keep="last")

                inc_q_small = inc_q[["fiscalDateEnding", "grossProfit"]].rename(
                    columns={"grossProfit": "grossProfit_quarterly"}
                )

                ratios = ratios.merge(inc_q_small, on="fiscalDateEnding", how="left")

                gp_q = ratios["grossProfit_quarterly"]
                ratios["qoq_gross_profit_log"] = np.log(gp_q.replace({0: np.nan})).diff()

                # ---------- Research and development ----------
                ratios["rnd_intensity"]   = rnd_ttm / revenue_ttm.replace(0, np.nan)
                ratios["rnd_to_opex"]     = rnd_ttm / df["operatingExpenses"].replace(0, np.nan)
                ratios["rnd_efficiency"]  = gross_profit_ttm / rnd_ttm.replace(0, np.nan)
                ratios["rnd_to_assets"]   = rnd_ttm / total_assets_avg.replace(0, np.nan)
                ratios["rnd_shock"]       = df["researchAndDevelopment_q"] / rnd_ttm.replace(0, np.nan)

                # ---------- Long-term growth proxies ----------
                # Reinvestment rates
                ratios["reinvestment_rate_capex_only"] = capex_ttm / operating_cf_ttm.replace(0, np.nan)
                ratios["reinvestment_rate_capex_rnd"]  = (capex_ttm + rnd_ttm) / operating_cf_ttm.replace(0, np.nan)

                # Fundamentele LT growth proxy: ROIC × reinvestment rate
                ratios["lt_growth_roic_reinv"] = ratios["ROIC"] * ratios["reinvestment_rate_capex_rnd"]

                # YoY revenue growth (TTM) als extra LT-growth proxy
                revenue_ttm_sorted      = revenue_ttm.reset_index(drop=True)
                net_income_ttm_sorted   = net_income_ttm.reset_index(drop=True)

                ratios["yoy_revenue_ttm_log"] = (
                    np.log(revenue_ttm_sorted.replace({0: np.nan})) -
                    np.log(revenue_ttm_sorted.replace({0: np.nan}).shift(4))
                )

                ratios["yoy_net_income_ttm_log"] = (
                    np.log(net_income_ttm_sorted.replace({0: np.nan})) -
                    np.log(net_income_ttm_sorted.replace({0: np.nan}).shift(4))
                )

                # ---------- Data voor valuation/prijs-gerelateerde ratios ----------
                ratios["commonStockSharesOutstanding_bsq"] = df.loc[ratios.index, "commonStockSharesOutstanding_bsq"]
                ratios["netDebtLastq"]   = net_debt_lastq
                ratios["netDebtAvg"]     = net_debt_avg
                ratios["net_income_ttm"] = net_income_ttm
                ratios["revenue_ttm"]    = revenue_ttm
                ratios["free_cf_ttm"]    = free_cf_ttm
                ratios["ebitda_ttm"]     = ebitda_ttm
                ratios["operating_cf_ttm"] = operating_cf_ttm
                ratios["dividends_ttm"]  = dividends_ttm_sorted.loc[ratios.index]
                ratios["capex"]          = capex_ttm
                ratios["totalShareholderEquity"] = shareholderEquity
                ratios["grossProfit"] = gross_profit_ttm
                ratios["ebit_ttm"]          = ebit_ttm
                ratios["rnd"]          = rnd_ttm



                # Laatste stap: eventueel nieuwste periode bovenaan
                # ratios = ratios.sort_values("fiscalDateEnding", ascending=False).reset_index(drop=True)

                return ratios



            # Get dataframe with ratios
            ratios_df = build_fundamental_ratios(ttm_inc, ttm_cf, bs_avg, bs_quarterly, inc_quarterly)

            # # Sorteer op datum oplopend voor alle growth berekeningen
            # ttm_eps = ttm_eps.sort_values("fiscalDateEnding").reset_index(drop=True)

            # # Get eps data
            # ratios_df = ratios_df.merge(
            #     ttm_eps,
            #     on="fiscalDateEnding",
            #     how="inner",
            #     suffixes=("", "_ttmcf")
            # )

            # ratios_df = ratios_df.merge(
            #     eps_quarterly[['surprisePercentage', 'fiscalDateEnding']],
            #     on="fiscalDateEnding",
            #     how="inner",
            #     suffixes=("", "_perQuarter")
            # )



            # # Lag estimated EPS
            # ratios_df['estimatedEPS'] = ratios_df['estimatedEPS'].shift(-1)



            ###############################################################################
            # Get data yahoo finance

            # Get ticker data
            ticker = yf.Ticker(SYMBOL)

            # Get prices
            prices = ticker.history(start="2010-07-01", end=dt.datetime.today().strftime("%Y-%m-%d"))

            # Get general information
            info = ticker.info

            # Add industry
            ratios_df['industry'] = info['industry']



            # deze herdoen want nu zijn ze allemaal NA

            ratios_df['estimatedEPS'] = np.nan
            ratios_df['reportedEPS'] = np.nan

            # waar estimated EPS is nan => forward EPS yfinance
            ratios_df['estimatedEPS'][-1] = info['epsForward']
            ratios_df['reportedEPS'][-1] = info['epsCurrentYear']





            ###############################################################################
            # lag variables QoQ and YoY using relative percentage differences

            cols = ratios_df.columns.tolist()

            to_remove = ['industry', 'fiscalDateEnding', 'reportedCurrency', 'fcf_growth_log',
                         'yoy_gross_profit_ttm_log', 'dividend_growth_log', 'qoq_gross_profit_log',
                         'yoy_revenue_ttm_log', 'yoy_net_income_ttm_log', 'commonStockSharesOutstanding_bsq',
                         'netDebtLastq', 'netDebtAvg', 'reportedDate', 'reportTime']  # columns you want to delete from the list

            cols = [c for c in cols if c not in to_remove]

            # QoQ
            for v in cols:
                ratios_df[f'{v}_QoQ'] = ratios_df[f'{v}'].pct_change()

            # YoY
            for v in cols:
                ratios_df[f'{v}_YoY'] = ratios_df[f'{v}'].pct_change(periods=4)

            # Changes for estimated eps and actual eps
            ratios_df['estimated_vs_current_eps'] = (ratios_df['estimatedEPS'] - ratios_df['reportedEPS']) / abs(ratios_df['reportedEPS'])


            # Hanlde changes with crossings



            ###############################################################################
            # Merge price data with ratios_df

            ratios_df['reportedDate'] = pd.to_datetime(ratios_df['reportedDate'], errors='coerce')
            prices['priceDate'] = pd.to_datetime(prices.index, errors='coerce')

            # timezone verwijderen indien aanwezig
            if getattr(prices['priceDate'].dt, 'tz', None) is not None:
                prices['priceDate'] = prices['priceDate'].dt.tz_localize(None)

            if getattr(ratios_df['reportedDate'].dt, 'tz', None) is not None:
                ratios_df['reportedDate'] = ratios_df['reportedDate'].dt.tz_localize(None)

            # forceer exact hetzelfde datetime-type
            ratios_df['reportedDate'] = ratios_df['reportedDate'].astype('datetime64[ns]')
            prices['priceDate'] = prices['priceDate'].astype('datetime64[ns]')

            # verwijder lege datums
            ratios_df = ratios_df.dropna(subset=['reportedDate'])
            prices = prices.dropna(subset=['priceDate'])

            ratios_df = ratios_df.sort_values('reportedDate')
            prices = prices.sort_values('priceDate')

            merged_df = pd.merge_asof(
                prices,
                ratios_df,
                left_on='priceDate',
                right_on='reportedDate',
                direction='backward'
            )

            merged_df = merged_df.set_index('priceDate')

            # extra variabelen: aantal dagen tot volgende earnings release
            merged_df['daysSinceRelease'] = (
                merged_df.index - merged_df['reportedDate']
            ).dt.days



            # Get ratios with market cap
            merged_df['marketCap'] = merged_df['Close'] * merged_df['commonStockSharesOutstanding_bsq']

            merged_df['net_income_ttm/marketCap'] = merged_df['net_income_ttm'] / merged_df['marketCap']
            merged_df['revenue_ttm/marketCap'] = merged_df['revenue_ttm'] / merged_df['marketCap']
            merged_df['estimatedEPS/Close'] = merged_df['estimatedEPS'] / merged_df['Close']
            merged_df['free_cf_ttm/marketCap'] = merged_df['free_cf_ttm'] / merged_df['marketCap']
            merged_df['operating_cf_ttm/marketCap'] = merged_df['operating_cf_ttm'] / merged_df['marketCap']
            # merged_df['reportedEPS/marketCap'] = merged_df['reportedEPS'] / merged_df['marketCap']
            # merged_df['estimatedEPS/marketCap'] = merged_df['estimatedEPS'] / merged_df['marketCap']
            merged_df['reportedEPS/marketCap'] = merged_df['reportedEPS'] / merged_df['Close']
            merged_df['estimatedEPS/marketCap'] = merged_df['estimatedEPS'] / merged_df['Close']
            merged_df['dividends_ttm/marketCap'] = merged_df['dividends_ttm'] / merged_df['marketCap']
            merged_df['bookValue/marketCap'] = merged_df['totalShareholderEquity'] / merged_df['marketCap']


            # Get ratios with enterprise value
            merged_df['enterprisevalue'] = merged_df['marketCap'] + merged_df['netDebtLastq']

            merged_df['free_cf_ttm/enterprisevalue'] = merged_df['free_cf_ttm'] / merged_df['enterprisevalue']
            merged_df['operating_cf_ttm/enterprisevalue'] = merged_df['operating_cf_ttm'] / merged_df['enterprisevalue']
            merged_df['ebitda_ttm/enterprisevalue'] = merged_df['ebitda_ttm'] / merged_df['enterprisevalue']
            merged_df['ebit_ttm/enterprisevalue'] = merged_df['ebit_ttm'] / merged_df['enterprisevalue']
            merged_df['capex/enterprisevalue'] = merged_df['capex'] / merged_df['enterprisevalue']
            merged_df['revenue_ttm/enterprisevalue'] = merged_df['revenue_ttm'] / merged_df['enterprisevalue']


            cols = ['net_income_ttm/marketCap', 'revenue_ttm/marketCap', 'free_cf_ttm/marketCap', 'operating_cf_ttm/marketCap',
                    'reportedEPS/marketCap', 'estimatedEPS/marketCap', 'dividends_ttm/marketCap',
                    'bookValue/marketCap', 'enterprisevalue', 'free_cf_ttm/enterprisevalue',
                    'operating_cf_ttm/enterprisevalue', 'ebitda_ttm/enterprisevalue', 'ebit_ttm/enterprisevalue',
                    'capex/enterprisevalue', 'revenue_ttm/enterprisevalue', 'estimatedEPS/Close']

            # deze moeten nog gelagged worden??
            for v in cols:
                merged_df[f'{v}_DoD'] = merged_df[f'{v}'].pct_change()


            ###############################################################################
            # Get rolling beta

            merged_df.index = pd.to_datetime(merged_df.index).tz_localize(None)
            SP500_prices.index = pd.to_datetime(SP500_prices.index).tz_localize(None)

            # --- 3) Compute daily returns ---
            stock_ret = merged_df['Close'].pct_change().rename("stock_ret")
            mkt_ret   = SP500_prices['Close'].pct_change().rename("mkt_ret")

            # --- 4) Align & drop missing dates ---
            ret_df = pd.concat([stock_ret, mkt_ret], axis=1).dropna()

            # --- 5) Rolling beta ---
            window = 60  # e.g. 60 trading days; change to 126/252 if you want
            beta = (
                ret_df["stock_ret"].rolling(window).cov(ret_df["mkt_ret"])
                / ret_df["mkt_ret"].rolling(window).var()
            )

            # --- 6) Add back to merged_df ---
            merged_df[f"beta_{window}d"] = beta.reindex(merged_df.index)

            # --- 5) Rolling beta ---
            window = 120  # e.g. 60 trading days; change to 126/252 if you want
            beta = (
                ret_df["stock_ret"].rolling(window).cov(ret_df["mkt_ret"])
                / ret_df["mkt_ret"].rolling(window).var()
            )

            # --- 6) Add back to merged_df ---
            merged_df[f"beta_{window}d"] = beta.reindex(merged_df.index)

            # --- 5) Rolling beta ---
            window = 240  # e.g. 60 trading days; change to 126/252 if you want
            beta = (
                ret_df["stock_ret"].rolling(window).cov(ret_df["mkt_ret"])
                / ret_df["mkt_ret"].rolling(window).var()
            )

            # --- 6) Add back to merged_df ---
            merged_df[f"beta_{window}d"] = beta.reindex(merged_df.index)

            # Add beta spread
            merged_df["beta_spread_60_240"] = merged_df["beta_60d"] - merged_df["beta_240d"]


            ###############################################################################
            # Technical indicators
            ###############################################################################
            # momentum

            px = merged_df['Close']

            # trading-day approximations
            d1, d3, d6 = 21, 63, 126

            # 3-1M momentum: return from (t-3M) -> (t-1M)
            merged_df["mom_3_1M"] = px.shift(d1) / px.shift(d3) - 1

            # 6-1M momentum: return from (t-6M) -> (t-1M)
            merged_df["mom_6_1M"] = px.shift(d1) / px.shift(d6) - 1

            # 1-3M momentum (common definition): short-term vs medium-term momentum
            # = 1M return minus 3M return (often used as a reversal / acceleration signal)
            ret_1M = px / px.shift(d1) - 1
            ret_3M = px / px.shift(d3) - 1
            merged_df["mom_1_3M"] = ret_1M - ret_3M


            # --- Reversal indicators ---
            # (often defined as the negative of recent returns: strong recent gain -> negative reversal signal)
            merged_df["rev_1w"] = -(px.pct_change(5))     # 1-week reversal
            merged_df["rev_1m"] = -(px.pct_change(21))    # 1-month reversal

            # --- 52-week high and distance to it ---
            high_52w = px.rolling(252, min_periods=252).max()
            #merged_df["high_52w"] = high_52w

            # Distance to 52W high: (Price / High) - 1  -> 0 at the high, negative below the high
            merged_df["dist_to_52w_high"] = (px / high_52w) - 1

            ###############################################################################
            # trend

            # pick price column
            price_col = "close" if "close" in merged_df.columns else "Close"
            px = merged_df[price_col].astype(float)

            # --- Moving averages ---
            ma_50  = px.rolling(50, min_periods=50).mean()
            ma_200 = px.rolling(200, min_periods=200).mean()

            # --- Moving average trend features ---
            # Price vs MA200 (percentage distance)
            merged_df["px_vs_ma200"] = (px / ma_200) - 1

            # MA50 - MA200 spread (percentage; scale-free)
            merged_df["ma50_ma200_spread"] = (ma_50 / ma_200) - 1

            # --- Trend strength: slope of log price over 63d ---
            # slope in "log-price units per day" (bigger = stronger uptrend)
            log_px = np.log(px)

            def slope(x: np.ndarray) -> float:
                x = np.asarray(x, dtype="float64")
                if np.any(np.isnan(x)):
                    return np.nan
                t = np.arange(len(x), dtype="float64")
                # OLS slope = cov(t,x)/var(t)
                return np.cov(t, x, ddof=0)[0, 1] / np.var(t)

            merged_df["logpx_slope_63d"] = log_px.rolling(63, min_periods=63).apply(slope, raw=True)



            ###############################################################################
            # volatility

            # daily returns (log returns are common for vol)
            ret = np.log(px).diff()

            ann = np.sqrt(252)

            # --- Realised volatility (annualised) ---
            merged_df["rv_20d"] = ret.rolling(20, min_periods=20).std() * ann
            merged_df["rv_60d"] = ret.rolling(60, min_periods=60).std() * ann

            # --- Vol-of-vol (regime / instability indicator) ---
            # rolling std of realised vol itself
            merged_df["vov_20d"] = merged_df["rv_20d"].rolling(20, min_periods=20).std()
            merged_df["vov_60d"] = merged_df["rv_60d"].rolling(60, min_periods=60).std()

            # Optional: relative vol-of-vol (scale-free)
            merged_df["vov_20d_rel"] = merged_df["vov_20d"] / merged_df["rv_20d"]
            merged_df["vov_60d_rel"] = merged_df["vov_60d"] / merged_df["rv_60d"]

            ###############################################################################
            # Drawdown

            # --- auto-detect column names ---
            close_col = "close" if "close" in merged_df.columns else "Close"
            high_col  = "high"  if "high"  in merged_df.columns else "High"
            low_col   = "low"   if "low"   in merged_df.columns else "Low"

            px = merged_df[close_col]

            # --- helper: max drawdown inside a window (returns a negative number, e.g., -0.27 = -27%) ---
            def max_drawdown_window(arr: np.ndarray) -> float:
                arr = np.asarray(arr, dtype="float64")
                if np.all(np.isnan(arr)):
                    return np.nan
                peak = np.maximum.accumulate(arr)
                dd = arr / peak - 1.0
                return np.nanmin(dd)

            # 3M and 6M rolling windows (trading-day approximations)
            w3m, w6m = 63, 126

            merged_df["mdd_3m"] = px.rolling(w3m, min_periods=w3m).apply(max_drawdown_window, raw=True)
            merged_df["mdd_6m"] = px.rolling(w6m, min_periods=w6m).apply(max_drawdown_window, raw=True)

            # --- ATR(14) ---
            # True Range (TR)
            prev_close = merged_df[close_col].shift(1)
            tr = pd.concat([
                (merged_df[high_col] - merged_df[low_col]).abs(),
                (merged_df[high_col] - prev_close).abs(),
                (merged_df[low_col] - prev_close).abs()
            ], axis=1).max(axis=1)

            # ATR(14): Wilder's smoothing (common)
            atr_14 = tr.ewm(alpha=1/14, adjust=False, min_periods=14).mean()

            # Normalized ATR: ATR / price
            merged_df["atr14_norm"] = atr_14 / merged_df[close_col]


            ###############################################################################
            # Volume, liquidity and attention

            # ---- column detection ----
            price_col = "close" if "close" in merged_df.columns else "Close"
            vol_col   = "volume" if "volume" in merged_df.columns else "Volume"

            px = merged_df[price_col].astype(float)
            vol = merged_df[vol_col].astype(float)

            # ---- Dollar Volume ----
            dollar_volume = px * vol

            # 20d average dollar volume
            merged_df["dollar_volume_20d_avg"] = dollar_volume.rolling(20, min_periods=20).mean()

            # Dollar volume trend (2 opties; kies wat je prefereert)
            # Optie A (simpel & interpreteerbaar): log-ratio vs 60d baseline
            dollar_volume_60d_avg = dollar_volume.rolling(60, min_periods=60).mean()
            merged_df["dollar_volume_trend"] = np.log(
                (merged_df["dollar_volume_20d_avg"] / dollar_volume_60d_avg).replace(0, np.nan)
            )

            # ---- Volume surprise / attention: z-score (20d) ----
            vol_mean_20 = vol.rolling(20, min_periods=20).mean()
            vol_std_20  = vol.rolling(20, min_periods=20).std()

            merged_df["volume_z_20d"] = (vol - vol_mean_20) / vol_std_20.replace(0, np.nan)

            # ---- Abnormal dollar value (P x V): z-score (20d) ----
            dv = dollar_volume
            dv_mean_20 = dv.rolling(20, min_periods=20).mean()
            dv_std_20  = dv.rolling(20, min_periods=20).std()

            merged_df["dollar_volume_z_20d"] = (dv - dv_mean_20) / dv_std_20.replace(0, np.nan)

            # ---- Amihud illiquidity (20d) ----
            # Amihud ~ avg_t( |return_t| / dollar_volume_t )
            ret = px.pct_change()
            amihud_daily = ret.abs() / dv.replace(0, np.nan)

            merged_df["amihud_20d"] = amihud_daily.rolling(20, min_periods=20).mean()



            ###############################################################################
            # price lags -1 day, -5, -10,...

            merged_df['price_lag_1'] = merged_df['Close'].pct_change(periods=1)
            merged_df['price_lag_5'] = merged_df['Close'].pct_change(periods=5)
            merged_df['price_lag_20'] = merged_df['Close'].pct_change(periods=20)
            merged_df['price_lag_60'] = merged_df['Close'].pct_change(periods=60)


            ###############################################################################
            # Target variables
            ###############################################################################
            # +20, +40, +60, +120

            # Targets: future returns over +20, +40, +60, +120 trading days
            merged_df['return_20']  = merged_df['Close'].shift(-20)  / merged_df['Close'] - 1
            merged_df['return_40']  = merged_df['Close'].shift(-40)  / merged_df['Close'] - 1
            merged_df['return_60']  = merged_df['Close'].shift(-60)  / merged_df['Close'] - 1
            merged_df['return_120'] = merged_df['Close'].shift(-120) / merged_df['Close'] - 1


            merged_df['Ticker'] = SYMBOL

            ticker_data_without_eps.append(SYMBOL)

            data_list.append(merged_df)

            time.sleep(30)


    else:
        empty_tickers_list.append(SYMBOL)
        time.sleep(30)







###############################################################################
# Get drawdowns

def add_future_min_low_pct(data_list, window=20, exclude_today=True, require_full_window=False):
    new_list = []

    for df in data_list:
        df = df.sort_index().copy()  # zorg dat tijd oplopend is

        low = df["Low"]
        close = df["Close"]

        # Als je "komende 20 dagen" EXCLUSIEF vandaag wil: shift(-1)
        if exclude_today:
            low_for_future = low.shift(-1)
        else:
            low_for_future = low

        minp = window if require_full_window else 1

        # future rolling min via "reverse rolling"
        future_min_low = (
            low_for_future.iloc[::-1]
            .rolling(window=window, min_periods=minp)
            .min()
            .iloc[::-1]
        )

        df[f"future_min_low_{window}"] = future_min_low
        df[f"pct_drop_to_future_min_low_{window}"] = (future_min_low / close) - 1

        new_list.append(df)

    return new_list

# Gebruik:
data_list = add_future_min_low_pct(
    data_list,
    window=20,
    exclude_today=True,        # t+1..t+20
    require_full_window=False  # laatste rijen: gebruikt wat er nog beschikbaar is
)

# Gebruik:
data_list = add_future_min_low_pct(
    data_list,
    window=40,
    exclude_today=True,        # t+1..t+20
    require_full_window=False  # laatste rijen: gebruikt wat er nog beschikbaar is
)

# Gebruik:
data_list = add_future_min_low_pct(
    data_list,
    window=60,
    exclude_today=True,        # t+1..t+20
    require_full_window=False  # laatste rijen: gebruikt wat er nog beschikbaar is
)

# Gebruik:
data_list = add_future_min_low_pct(
    data_list,
    window=120,
    exclude_today=True,        # t+1..t+20
    require_full_window=False  # laatste rijen: gebruikt wat er nog beschikbaar is
)


def add_future_max_high_pct(data_list, window=20, exclude_today=True, require_full_window=False):
    new_list = []

    for df in data_list:
        df = df.sort_index().copy()  # zorg dat tijd oplopend is

        high = df["High"]
        close = df["Close"]

        # Als je "komende dagen" EXCLUSIEF vandaag wil: shift(-1)
        if exclude_today:
            high_for_future = high.shift(-1)
        else:
            high_for_future = high

        minp = window if require_full_window else 1

        # future rolling max via "reverse rolling"
        future_max_high = (
            high_for_future.iloc[::-1]
            .rolling(window=window, min_periods=minp)
            .max()
            .iloc[::-1]
        )

        df[f"future_max_high_{window}"] = future_max_high
        df[f"pct_up_to_future_max_high_{window}"] = (future_max_high / close) - 1

        new_list.append(df)

    return new_list

# Gebruik:
data_list = add_future_max_high_pct(
    data_list,
    window=20,
    exclude_today=True,        # t+1..t+120
    require_full_window=False  # laatste rijen: gebruikt wat er nog beschikbaar is
)

# Gebruik:
data_list = add_future_max_high_pct(
    data_list,
    window=40,
    exclude_today=True,        # t+1..t+120
    require_full_window=False  # laatste rijen: gebruikt wat er nog beschikbaar is
)

# Gebruik:
data_list = add_future_max_high_pct(
    data_list,
    window=60,
    exclude_today=True,        # t+1..t+120
    require_full_window=False  # laatste rijen: gebruikt wat er nog beschikbaar is
)

# Gebruik:
data_list = add_future_max_high_pct(
    data_list,
    window=120,
    exclude_today=True,        # t+1..t+120
    require_full_window=False  # laatste rijen: gebruikt wat er nog beschikbaar is
)


###############################################################################
# Combine dataframes and export


combined_df = pd.concat(data_list, axis=0, ignore_index=False)


# voorbeeld: exporteer df naar een specifiek pad
output_path = "output.xlsx"

combined_df.to_excel(output_path, index=True)
