
for epoch in Current 2050; do
    gdalwarp \
        -te 87.932274 20.648357 92.735326 26.67918 \
        -te_srs "EPSG:4326" \
        LS_NbS_${epoch}_9s.tif \
        ../../../Bangladesh\ data/nature-ecosystems/LandslideRiskReductionNbS/LS_NbS_${epoch}_9s_bgd.tif
done

