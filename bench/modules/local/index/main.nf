process INDEX {
    label 'process_bcftools'
    cpus 2

    input:
    path(vcf)

    output:
    tuple path(vcf), path("*.tbi"), path("*.csi"), path("*.gzi"), path("*.bcf"), path("*.bcf.csi"), emit: indexed

    script:
    def prefix = vcf.baseName.replaceAll(/\.vcf$/, '')
    """
    # TBI index
    if [ ! -f "${vcf}.tbi" ]; then
        tabix -p vcf "${vcf}"
    else
        cp "${vcf}.tbi" ./ 2>/dev/null || true
    fi

    # CSI index
    if [ ! -f "${vcf}.csi" ]; then
        bcftools index --csi "${vcf}"
    else
        cp "${vcf}.csi" ./ 2>/dev/null || true
    fi

    # GZI index
    if [ ! -f "${vcf}.gzi" ]; then
        bgzip --reindex "${vcf}"
    else
        cp "${vcf}.gzi" ./ 2>/dev/null || true
    fi

    # BCF conversion + CSI index
    if [ ! -f "${prefix}.bcf" ]; then
        bcftools view -Ob -o "${prefix}.bcf" --threads ${task.cpus} "${vcf}"
        bcftools index --csi "${prefix}.bcf"
    else
        cp "${prefix}.bcf" ./ 2>/dev/null || true
        cp "${prefix}.bcf.csi" ./ 2>/dev/null || true
    fi
    """
}
