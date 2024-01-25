import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

from src.sensortower.intel import getAndroidDownloadAndRevenue
from collections import defaultdict

# 通过竞品的国家下载量分布，与topwar进行对比
# 找到有潜力的国家
def main():
    appList = [
        'com.topgamesinc.evony',
        'com.gof.global',
        'com.topwar.gp',
        'com.igg.android.lordsmobile',
        'com.kingsgroup.sos',
        'com.lilithgame.roc.gp',
        'com.camelgames.aoz',
        'com.im30.ROE.gp',
        'com.yottagames.mafiawar',
        'com.more.dayzsurvival.gp',
        'com.funplus.kingofavalon',
        'com.yottagames.gameofmafia',
        'com.sivona.stormshot.e',
        'com.camelgames.superking',
        'com.diandian.gog',
        'com.totalbattle',
        'com.scopely.startrek',
        'com.star.union.planetant',
        'com.farlightgames.samo.gp',
        'com.igg.android.doomsdaylastsurvivors',
        'com.elex.twdsaw.gp',
        'com.global.antgame',
        'jp.co.koeitecmo.Power',
        'com.hcg.cok.gp',
        'com.wondergames.warpath.gp',
        'com.qookkagame.sgzzlb.gp.jp',
        'com.lilithgames.rok.gpkr',
        'com.tap4fun.kissofwar.googleplay',
        'com.tap4fun.ape.gplay',
        'leyi.westgame',
        'com.igg.android.vikingriseglobal',
        'com.plarium.vikings',
        'com.innogames.foeandroid',
        'com.netease.lotr',
        'com.funplus.mc',
        'com.bbgame.sgzapk.tw',
        'com.joycity.gw',
        'com.wb.goog.got.conquest',
        'com.kingsgroup.ss.jp',
        'com.bbgame.nobunaga.gp.jp',
        'com.special.warship',
        'jp.co.koeitecmo.Might',
        'com.doradogames.conflictnations.worldwar3',
        'com.sixwaves.shinsengoku',
        'com.joycity.potc',
        'com.allstarunion.beastlord',
        'com.longtech.lastwars.gp',
        'com.sialiagames.sgzzlb.gp.tw',
        'com.oasis.immortal',
        'com.lilithgames.rok.gp.jp',
        'com.allstarunion.ta.jp',
        'com.jedigames.p20.googleplay',
        'com.farlightgames.samo.gp.kr',
        'com.bigbreakgames.wot',
        'leyi.marsactionpro',
        'com.bytro.supremacy1914',
        'com.special.thewolfgame',
        'com.fingerfun.coc.gplay',
        'com.qookkagame.sgzzlb.gp.kr',
        'com.yottagames.stoneage',
        'com.jdgames.p20n.googleplay',
        'com.epicactiononline.ffxv.ane',
        'com.and.riseofthekings',
        'com.yottagames.gameofmafiajp',
        'com.es.civilization.rise.empire',
        'com.global.aoempires',
        'air.com.goodgamestudios.empirefourkingdoms',
        'com.herogame.gplay.rpg.sangokushi.koei.mobile',
        'com.qookkagame.sgzzlb.hkmo',
        'com.kingsgroup.ww2',
        'com.leme.coe',
        'com.machinezone.ffane',
        'and.onemt.war.ar',
        'com.yuedong.app.codgp1',
        'com.gameloft.android.ANMP.GloftGHHM',
        'com.entropy.global',
        'com.rok.gp.vn',
        'com.special.thewolfgame.asia',
        'com.machinezone.gow',
        'com.epicwaronline.ms',
        'com.geeker.gok',
        'com.topjoy.tkw.hk',
        'com.nirvanagames.nexuswar',
        'skydragon.honorofkings',
        'com.tap4fun.brutalage_test',
        'com.kingsgroup.ss.kr',
        'com.sixwaves.sangokuhaouden',
        'com.soosg.gp',
        'com.babil.androidpanzer',
        'com.allstarunion.ta.kr',
        'com.yottagames.gameofmafiakr',
        'com.sevenpirates.infinitywar',
        'com.bytro.callofwar1942',
        'com.tap4fun.reignofwar',
        'com.allstarunion.ta.tw',
        'kr.co.angames.astrokings.google.android',
        'com.erepubliklabs.warandpeace',
        'com.netease.lagrange',
        'com.innogames.riseofcultures',
        'and.onemt.boe.tr',
    ]

    country_counts = defaultdict(int)
    topwar_gp_countries = []

    N = 20

    for app in appList:
        # df = getAndroidDownloadAndRevenue(app,startDate='2023-01-01',endDate='2023-12-31')
        # df = df.groupby(['country']).agg({'downloads':'sum'}).reset_index()
        # df = df.sort_values(['downloads'],ascending=False).reset_index(drop=True)
        # df['rate'] = df['downloads']/df['downloads'].sum()

        # print(app)
        # print(df)

        # df.to_csv('/src/data/{}_downloads.csv'.format(app),index=False)
        df = pd.read_csv('/src/data/{}_downloads.csv'.format(app))
        
        # Update country counts and store top N countries for 'com.topwar.gp'
        for index, row in df.head(N).iterrows():
            country_counts[row['country']] += 1
            if app == 'com.topwar.gp':
                topwar_gp_countries.append(row['country'])

    # Find top N countries by count
    top_countries = sorted(country_counts.items(), key=lambda x: x[1], reverse=True)[:N]

    # Check if any top N countries are not in 'com.topwar.gp' top N countries
    not_in_topwar_gp = [country for country, _ in top_countries if country not in topwar_gp_countries]

    print("Top N countries:", top_countries)
    print("Top N countries not in 'com.topwar.gp':", not_in_topwar_gp)



if __name__ == '__main__':
    main()