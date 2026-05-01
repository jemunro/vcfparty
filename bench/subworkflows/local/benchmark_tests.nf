include { BASELINE }       from '../../modules/local/baseline'
include { BLOCKY_RUN }    from '../../modules/local/blocky_run'
include { SCATTER_GATHER } from '../../modules/local/scatter_gather'
include { BLOCKY_SCATTER } from '../../modules/local/blocky_scatter'

def buildCmds(test, out_flag, threads, out_ext, input) {
    def baseline, blocky, shard

    if (test == 'norm') {
        baseline = "bcftools norm -m-any --threads ${threads} ${out_flag} -o output.${out_ext} ${input}"
        blocky   = "bcftools norm -m-any --threads 0 ${out_flag}"
        shard    = "bcftools norm -m-any --threads 0 ${out_flag} -o proc_\$shard \$shard"
    } else if (test == 'fill_tags') {
        baseline = "bcftools +fill-tags --threads ${threads} ${out_flag} -o output.${out_ext} ${input} -- -t AC,AF,AN"
        blocky   = "bcftools +fill-tags --threads 0 ${out_flag} -- -t AC,AF,AN"
        shard    = "bcftools +fill-tags --threads 0 ${out_flag} -o proc_\$shard \$shard -- -t AC,AF,AN"
    } else if (test == 'cat') {
        baseline = "blocky decompress -c ${input} | cat | blocky compress > output.${out_ext}"
        blocky   = "cat"
        shard    = "blocky decompress -c \$shard | cat | blocky compress > proc_\$shard"
    } else if (test == 'bgzip') {
        baseline = "blocky decompress -c ${input} | bgzip --threads ${threads} > output.${out_ext}"
        blocky   = "bgzip"
        shard    = "blocky decompress -c \$shard | bgzip > proc_\$shard"
    } else if (test == 'cat4') {
        baseline = "blocky decompress -c ${input} | cat | cat | cat | cat | blocky compress > output.${out_ext}"
        blocky   = "cat ::: cat ::: cat ::: cat"
        shard    = "blocky decompress -c \$shard | cat | cat | cat | cat | blocky compress > proc_\$shard"
    } else if (test == 'vcfanno') {
        baseline = "vcfanno -p ${threads} vcfanno.conf ${input} | bgzip > output.${out_ext}"
        blocky   = "vcfanno -p 1 vcfanno.conf /dev/stdin"
        shard    = "vcfanno -p 1 vcfanno.conf \$shard | bgzip > proc_\$shard"
    }

    return [baseline: baseline, blocky: blocky, shard: shard]
}

workflow BENCHMARK_TESTS {
    take:
    indexed         // tuple(vcf, tbi, csi, gzi, bcf, bcf_csi)
    vcfanno_bundle  // tuple(conf, [clinvar_vcf, clinvar_csi])

    main:
    // Test groups
    def bcf_tests = ['norm', 'fill_tags']
    def vcf_tests = ['cat', 'bgzip', 'cat4']

    // Extract input channels
    ch_vcf = indexed.map { vcf, _tbi, csi, _gzi, _bcf, _bcf_csi -> tuple('vcf', vcf, csi) }
    ch_bcf = indexed.map { _vcf, _tbi, _csi, _gzi, bcf, bcf_csi -> tuple('bcf', bcf, bcf_csi) }

    // Unpack vcfanno bundle
    ch_bundle = vcfanno_bundle.first()

    // --- Build cross products ---

    // BCF tests: test x rep x (vcf + bcf) x ncpus
    ch_bcf_base = channel.of(bcf_tests).flatten()
        .combine(channel.of(1..params.nreps).flatten())
        .combine(ch_vcf.mix(ch_bcf))
        .combine(channel.of(params.ncpus).flatten())

    // VCF-only tests: test x rep x vcf x ncpus
    ch_vcf_base = channel.of(vcf_tests).flatten()
        .combine(channel.of(1..params.nreps).flatten())
        .combine(ch_vcf)
        .combine(channel.of(params.ncpus).flatten())

    // vcfanno: rep x vcf x ncpus x bundle
    ch_vcfanno_base = params.run_vcfanno
        ? channel.of('vcfanno')
            .combine(channel.of(1..params.nreps).flatten())
            .combine(ch_vcf)
            .combine(channel.of(params.ncpus).flatten())
            .combine(ch_bundle)
        : channel.empty()

    // All non-vcfanno tests combined
    ch_base = ch_bcf_base.mix(ch_vcf_base)
    // -> [test, rep, fmt, input, idx, ncpus]

    // --- Baseline ---
    // Filter: cat/cat4 native capped at ncpus <= 4
    ch_baseline = ch_base
        .filter { test, _rep, _fmt, _input, _idx, ncpus ->
            !(test in ['cat', 'cat4'] && ncpus > 4)
        }
        .map { test, rep, fmt, input, idx, ncpus ->
            def out_flag = fmt == 'bcf' ? '-Ob' : '-Oz'
            def threads  = ncpus == 1 ? 0 : ncpus
            def out_ext  = fmt == 'bcf' ? 'bcf' : 'vcf.gz'
            def cmds = buildCmds(test, out_flag, threads, out_ext, input)
            def meta = [test: test, mode: 'native', format: fmt, rep: rep, ncpus: ncpus, nthreads: threads]
            tuple(meta, cmds.baseline, input, idx, [])
        }

    // vcfanno baseline
    ch_baseline_vcfanno = ch_vcfanno_base.map { test, rep, fmt, input, idx, ncpus, conf, sources ->
        def cmds = buildCmds('vcfanno', '-Oz', ncpus, 'vcf.gz', input.name)
        def meta = [test: 'vcfanno', mode: 'native', format: 'vcf', rep: rep, ncpus: ncpus, nthreads: ncpus]
        tuple(meta, cmds.baseline, input, idx, [conf] + sources)
    }

    BASELINE(ch_baseline.mix(ch_baseline_vcfanno))

    // --- Blocky run ---
    ch_blocky = ch_base.map { test, rep, fmt, input, idx, ncpus ->
        def out_flag = fmt == 'bcf' ? '-Ob' : '-Oz'
        def out_ext  = fmt == 'bcf' ? 'bcf' : 'vcf.gz'
        def cmds = buildCmds(test, out_flag, 0, out_ext, input.name)
        def meta = [test: test, mode: 'blocky', format: fmt, rep: rep, ncpus: ncpus, nthreads: 1]
        tuple(meta, cmds.blocky, input, idx, [])
    }

    ch_blocky_vcfanno = ch_vcfanno_base.map { test, rep, fmt, input, idx, ncpus, conf, sources ->
        def cmds = buildCmds('vcfanno', '-Oz', 0, 'vcf.gz', input.name)
        def meta = [test: 'vcfanno', mode: 'blocky', format: 'vcf', rep: rep, ncpus: ncpus, nthreads: 1]
        tuple(meta, cmds.blocky, input, idx, [conf] + sources)
    }

    BLOCKY_RUN(ch_blocky.mix(ch_blocky_vcfanno))

    // --- Scatter/gather ---
    ch_results = BASELINE.out.results
        .mix(BLOCKY_RUN.out.results)

    if (params.run_scatter_gather) {
        ch_sg = ch_base
            .map { test, rep, fmt, input, idx, ncpus ->
                def out_flag = fmt == 'bcf' ? '-Ob' : '-Oz'
                def out_ext  = fmt == 'bcf' ? 'bcf' : 'vcf.gz'
                def cmds = buildCmds(test, out_flag, 0, out_ext, input.name)
                def meta = [test: test, mode: 'scatter_gather', format: fmt, rep: rep, ncpus: ncpus, nthreads: 1]
                tuple(meta, cmds.shard, input, idx, [])
            }

        ch_sg_vcfanno = ch_vcfanno_base.map { test, rep, fmt, input, idx, ncpus, conf, sources ->
            def cmds = buildCmds('vcfanno', '-Oz', 0, 'vcf.gz', input.name)
            def meta = [test: 'vcfanno', mode: 'scatter_gather', format: 'vcf', rep: rep, ncpus: ncpus, nthreads: 1]
            tuple(meta, cmds.shard, input, idx, [conf] + sources)
        }

        SCATTER_GATHER(ch_sg.mix(ch_sg_vcfanno))
        ch_results = ch_results.mix(SCATTER_GATHER.out.results)
    }

    // --- Scatter index comparison: VCF only, always n=4 ---
    ch_scatter = channel.of('csi', 'tbi', 'gzi', 'none')
        .combine(channel.of(1..params.nreps).flatten())
        .combine(indexed.map { vcf, tbi, csi, gzi, _bcf, _bcf_csi ->
            tuple(vcf, tbi, csi, gzi)
        })

    BLOCKY_SCATTER(ch_scatter)

    ch_results = ch_results.mix(BLOCKY_SCATTER.out.results)

    emit:
    results = ch_results
}
