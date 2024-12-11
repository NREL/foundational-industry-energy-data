import pandas as pd
from pathlib import Path



def load_naics_assignments(og_fp, new_fp):
    """
    Analysis of NAICS code assignments between original FIED 
    and revised approach.
    """



    og_naics = pd.read_csv(
        og_fp,
        usecols=['registryID', 'naicsCode'], 
        low_memory=False
        )
    
    og_naics.drop_duplicates(subset=['registryID'], inplace=True)

    new_naics = pd.read_csv(
        new_fp, 
        usecols=['registryID', 'naicsCode'],
        low_memory=False
        )
    
    return og_naics, new_naics


def compare_naics_assignments(og_naics, new_naics):

    comparison = pd.merge(
        og_naics, new_naics,
        on='registryID',
        how='outer',
        suffixes=['_og', '_new']
        )
    
    comparison.loc[:, 'same'] = comparison.naicsCode_og == comparison.naicsCode_new
    
    # counts of new, counts by industry, counts of changed (%), total counts
    summary_dict = {
        'total_count': {
            'og': comparison.naicsCode_og.count(),
            'new': comparison.naicsCode_new.count()
            },
        'industry_count': {
            'og': comparison.naicsCode_og.dropna().apply(lambda x: str(int(x))[0:2]).value_counts(),
            'new': comparison.naicsCode_new.dropna().apply(lambda x: str(int(x))[0:2]).value_counts()
            },
        'same': {
            'abs': comparison.dropna().same.value_counts(),
            'rel': comparison.dropna().same.value_counts()/len(comparison.dropna())
            }
        }
    
    return summary_dict


if __name__ == '__main__':

    new_fp = Path(Path(__file__).parents[1], 'fied/data/FRS/frs_data_formatted.csv')
    og_fp =  Path(Path(__file__).home(), 'desktop/foundational_industry_data_2017.csv')

    og_naics, new_naics = load_naics_assignments(og_fp, new_fp)
    summary_dict = compare_naics_assignments(og_naics, new_naics)