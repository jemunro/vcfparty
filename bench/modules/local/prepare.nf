process PREPARE {
    label 'process_bcftools'
    cpus 2
    tag "prepare"

    input:
    path(vcf)

    output:
    tuple path("prepared.vcf.gz"), path("prepared.vcf.gz.tbi"), path("prepared.vcf.gz.csi"), path("prepared.vcf.gz.gzi"), path("prepared.bcf"), path("prepared.bcf.csi"), emit: indexed

    script:
    def n_samples = params.n_samples
    """
    # Subset to first N samples (if input has more)
    N_TOTAL=\$(bcftools query -l ${vcf} | wc -l)
    if [ "\$N_TOTAL" -gt "${n_samples}" ]; then
        bcftools query -l ${vcf} | head -n ${n_samples} > samples.txt
        bcftools view -S samples.txt --threads ${task.cpus} -Ob -o prepared.bcf ${vcf}
        rm samples.txt
    else
        bcftools view --threads ${task.cpus} -Ob -o prepared.bcf ${vcf}
    fi

    # VCF conversion
    bcftools view -Oz -o prepared.vcf.gz --threads ${task.cpus} prepared.bcf

    # Create all indices
    bcftools index --threads ${task.cpus} --csi prepared.bcf
    bcftools index --threads ${task.cpus} --csi prepared.vcf.gz
    bcftools index --threads ${task.cpus} --tbi prepared.vcf.gz
    bgzip --threads ${task.cpus} --reindex prepared.vcf.gz
    """
}
