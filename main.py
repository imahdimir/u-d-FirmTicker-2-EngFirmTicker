"""

    """

from pathlib import Path

import pandas as pd
from githubdata import GitHubDataRepo
from mirutil.df import save_as_prq_wo_index as sprq
from mirutil.ns import rm_ns_module
from mirutil.ns import update_ns_module
from mirutil.str import normalize_completley_and_rm_all_whitespaces as ncr
from mirutil.str import normalize_fa_str_completely as norm_fa

update_ns_module()
import ns

gdu = ns.GDU()
c = ns.Col()

class ColName :
    eftic = 'کد 4 رقمی شرکت'

cn = ColName()

def change_old_firmticker_to_new_firmticker(df) :
    gdm2f = GitHubDataRepo(gdu.src_m2f)
    gdm2f.clone_overwrite()
    dfb = gdm2f.read_data()

    dfb = dfb[[c.btic , c.ftic]]

    dfb[c.btic] = dfb[c.btic].apply(norm_fa)
    dfb = dfb.drop_duplicates()

    dfb = dfb.set_index(c.btic)

    df['norm'] = df[c.ftic].apply(norm_fa)

    df['nf'] = df['norm'].map(dfb[c.ftic])

    msk = df['nf'].notna()
    df.loc[msk , c.ftic] = df.loc[msk , 'nf']

    df = df.drop(columns = ['nf' , 'norm'])
    gdm2f.rmdir()

    return df

def main() :
    pass

    ##
    gduf = GitHubDataRepo(gdu.src_uf)
    gduf.clone_overwrite()

    ##
    dfu = gduf.read_data()

    ##
    gdm = GitHubDataRepo(gdu.src_m2f)
    gdm.clone_overwrite()

    ##
    dfm = gdm.read_data()

    ##
    dfm = dfm[[c.btic , c.ftic]]

    ##
    dfu[c.btic] = dfu[c.ftic]

    ##
    dfa = pd.concat([dfm , dfu] , axis = 0)

    ##
    dfa = dfa.drop_duplicates()

    ##
    gdi2s = GitHubDataRepo(gdu.src_i2s)
    gdi2s.clone_overwrite()

    ##
    dfi = gdi2s.read_data()

    ##
    dfi = dfi[[c.ftic , cn.eftic]]

    ##
    dfi = change_old_firmticker_to_new_firmticker(dfi)

    ##
    dfi = dfi.drop_duplicates()

    ##
    dfi = dfi.rename(columns = {
            cn.eftic : c.eng_ftic
            })

    ##
    dfi.to_csv('EngTicker-map.csv' , index = False)

    ##
    dfi[c.ftic] = dfi[c.ftic].apply(norm_fa)

    ##
    msk = dfi[c.ftic].duplicated(keep = False)
    df1 = dfi[msk]

    ##
    dfi = dfi.drop_duplicates()

    ##
    dfi = dfi.set_index(c.ftic)

    ##
    dfa[c.eng_ftic] = dfa[c.ftic].map(dfi[cn.eftic])

    ##

    ##
    dfi.columns

    ##
    dfa = df.copy()
    dfa['nt'] = dfa[c.name].apply(ncr)

    dfa = dfa.drop_duplicates(subset = ['nt' , c.ftic])

    ##
    msk = dfa['nt'].duplicated(keep = False)

    df1 = dfa[msk]

    assert dfa['nt'].is_unique , ermsg

    ##
    gdt = GitHubDataRepo(gdu.trg)
    gdt.clone_overwrite()

    ##
    dffp = gdt.local_path / 'data.prq'
    sprq(df , dffp)

    ##
    msg = 'Updated by: '
    msg += gdu.slf
    print(msg)

    ##
    gdt.commit_and_push(msg)

    ##
    gduf.rmdir()
    gdt.rmdir()

    rm_ns_module()

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')
