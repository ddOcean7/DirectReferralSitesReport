import pandas as pd
import requests
import pymongo
import json
import numpy as np
import xlsxwriter


# Pull in df
# df = pd.read_csv("Ocean_Daily_Usage_Report_2025-07-15.csv", header = 8)
df = pd.read_csv("Ocean_Daily_Usage_Report_2026-03-11.csv", header = 8)
# reference_df = pd.read_excel("paid_pe_usage_report-2025-07-15.xlsx")
reference_df = pd.read_excel("paid_pe_usage_2026_03_05.xlsx")

# Remove extra rows
# referral_df = df.iloc[8:len(df)-6].copy()

# Ensure 'Referrals Received' is numeric
referral_df["Referrals Received"] = pd.to_numeric(referral_df["Referrals Received"], errors='coerce')
referral_df["Site Number"] = pd.to_numeric(referral_df["Site Number"], errors='coerce')

# Ensure the target column is string
referral_df["RA siteNum covering Provider Network Licences"] = referral_df["RA siteNum covering Provider Network Licences"].astype(str)

# Apply filter conditions
filtered_referrals = referral_df[
    (referral_df["Referrals Received"] > 0) &
    (
        referral_df["RA siteNum covering Provider Network Licences"].isna() |
        (referral_df["RA siteNum covering Provider Network Licences"].str.strip() == "") |
        (referral_df["RA siteNum covering Provider Network Licences"].str.lower() == "nan")
    ) &
    (referral_df["Province"] == "ON") &
    (referral_df["Paying"] == "Y")
    ##(~referral_df["Site Name"].str.contains(r"demo|test", case=False, na=False))
]



#### Deal with DF 2 (Reference file) ####

# Focus only on site payers
reference_df_filtered = reference_df[reference_df["Site Payer?"] == "Yes"]

left = filtered_referrals.add_suffix("_usage")
left = left.rename(columns={"Site Number_usage": "Site Number"})
right = reference_df_filtered.add_suffix("_reference")
right = right.rename(columns={"Default Site Number_reference": "Default Site Number"})



# Left join on referral_df using Site Number
merged_df = pd.merge(
    left=left,
    right=right,
    how="left",
    left_on="Site Number",
    right_on="Default Site Number",
)

merged_df.drop(columns=["Default Site Number"], inplace=True)

# Add dupes column to indicate where site number occurs twice
merged_df["dupes"] = merged_df["Site Number"].duplicated(keep=False)

# Filter out any test/demo sites - save these to a second tab
second_tab = merged_df[merged_df["Site Name_usage"].str.contains(r"demo|test", case=False, na=False)]
merged_df2 = merged_df[~merged_df["Site Name_usage"].str.contains(r"demo|test", case=False, na=False)]





### PART 2: PULL FRESHSALES ACCOUNTS ###
account_view_ep = "https://cognisantmd2.myfreshworks.com/crm/sales/api/deals/scroll/16002522751"

api_key = "2fTPuq2FUHCInJTfkpvQ8g"

headers = {
    "Authorization" : f"Token token={api_key}",
    "Content-Type": "application/json"
}

response = requests.get(account_view_ep, headers=headers)
deals = response.json()["deals"]
meta = response.json()["meta"]
has_next = meta["has_next_page"]
deals_list = deals
requests_made = 1

while has_next:
    requests_made += 1
    scroll_ep = "https://cognisantmd2.myfreshworks.com/crm/sales/api/deals/scroll/16002522751"

    params = {
        "last_fetched_id": meta["last_fetched_id"],
    }

    response = requests.get(scroll_ep, params=params, headers=headers)
    print("Making request : " + str(requests_made) + "with status code" + str(response.status_code))

    deals = response.json()["deals"]
    deals_list.extend(deals)
    meta = response.json()["meta"]
    has_next = meta["has_next_page"]

deal_account_list = []
problem_deals = []
accounts_list = []
site_numbers = []
postal_codes = []

for deal in deals_list:
    deal_id = deal["id"]
    deal_ep = f"https://cognisantmd2.myfreshworks.com/crm/sales/api/deals/{deal_id}"

    params = {
        "include": "sales_account"
    }

    deal_response = requests.get(deal_ep, headers=headers, params=params)

    if len(deal_response.json()['sales_accounts']) > 1:
        print("More than one account found for deal : " + str(deal_id))
        problem_deals.append(deal_id)

    #Should only be one deal but if not grab the first
    account_id = deal_response.json()['sales_accounts'][0]['id']

    # Make req to account
    account_ep = f"https://cognisantmd2.myfreshworks.com/crm/sales/api/sales_accounts/{account_id}"
    account_response = requests.get(account_ep, headers=headers)

    accounts_list.extend([account_response.json()['sales_account']])

    site_number = account_response.json()['sales_account']['custom_field']['cf_site_number']
    postal_code = account_response.json()['sales_account']['zipcode']

    if postal_code == "" or postal_code is None:
        print("Dud postal code for account : " + str(account_id))
        postal_codes.append("")
    else:
        postal_codes.append(postal_code)

    site_numbers.append(site_number)

site_numbers2 = list(set(site_numbers))
site_numbers2 = [int(site_number) for site_number in site_numbers2]

sitenum_postal = pd.DataFrame({"site_number": site_numbers2, "postal_code": postal_codes})

### BACK TO THE DAILY USAGE REPORT ###

#This df shows all sites that are in common w the FreshSales view
fs_common_df = filtered_referrals[filtered_referrals["Site Number"].isin(site_numbers2)]

excluded_sites = set(filtered_referrals["Site Number"])

site_numbers_fs_focus = [
    site_number for site_number in site_numbers2
    if site_number not in excluded_sites
]

# Filter original df
fs_df = sitenum_postal[sitenum_postal["site_number"].isin(site_numbers_fs_focus)]

# Now left join with reference df
fs_df_merged = pd.merge(
    left = fs_df,
    right = reference_df_filtered,
    left_on = "site_number",
    right_on="Default Site Number",
)

# Drop extra site number column
fs_df_merged.drop("Default Site Number", axis=1, inplace=True)

# Write to Excel
with pd.ExcelWriter('DirectReferralReport2.xlsx', engine='xlsxwriter') as writer:
    merged_df2.to_excel(writer, sheet_name='Main', index=False)
    second_tab.to_excel(writer, sheet_name='TestDemoSites', index=False)
    fs_df_merged.to_excel(writer, sheet_name='FS Sites', index=False)



