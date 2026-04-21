include { BCFTOOLS_BASELINE } from '../../../modules/local/bcftools_baseline/main.nf'
include { BLOCKY_BCFTOOLS }  from '../../../modules/local/blocky_bcftools/main.nf'
include { BLOCKY_SCATTER }   from '../../../modules/local/blocky_scatter/main.nf'

workflow BENCHMARK_BCFTOOLS {
    take:
    indexed  // tuple(vcf, tbi, csi, gzi, bcf, bcf_csi)

    main:
    def tests = ['norm', 'fill_tags', 'norm_fill_tags']

    // Build input channels — native uses TBI for VCF, CSI for BCF
    ch_vcf_native = indexed.map { vcf, tbi, _csi, _gzi, _bcf, _bcf_csi ->
        tuple('vcf', vcf, tbi)
    }
    ch_bcf_native = indexed.map { _vcf, _tbi, _csi, _gzi, bcf, bcf_csi ->
        tuple('bcf', bcf, bcf_csi)
    }
    ch_native_inputs = ch_vcf_native.mix(ch_bcf_native)

    // Blocky always uses CSI index
    ch_vcf_blocky = indexed.map { vcf, _tbi, csi, _gzi, _bcf, _bcf_csi ->
        tuple('vcf', vcf, csi)
    }
    ch_bcf_blocky = indexed.map { _vcf, _tbi, _csi, _gzi, bcf, bcf_csi ->
        tuple('bcf', bcf, bcf_csi)
    }
    ch_blocky_inputs = ch_vcf_blocky.mix(ch_bcf_blocky)

    // Cross product: test x rep
    ch_design = channel.of(tests).flatten()
        .combine(channel.of(1..params.nreps).flatten())

    // --- Native (baseline + parallel) ---
    ch_native = ch_design
        .combine(ch_native_inputs)
        .combine(channel.of(1).mix(channel.of(params.ncpus).flatten()))
        .map { test, rep, fmt, input, idx, ncpus ->
            tuple(test, fmt, rep, ncpus, input, idx)
        }

    BCFTOOLS_BASELINE(ch_native)

    // --- Blocky: nthreads=1 for all ncpus, plus nthreads=2 for ncpus >= 4 ---
    ch_blocky_t1 = ch_design
        .combine(ch_blocky_inputs)
        .combine(channel.of(params.ncpus).flatten())
        .map { test, rep, fmt, input, idx, ncpus ->
            tuple(test, fmt, rep, ncpus, 1, input, idx)
        }

    ch_blocky_t2 = ch_design
        .combine(ch_blocky_inputs)
        .combine(channel.of(params.ncpus).flatten().filter { it >= 4 })
        .map { test, rep, fmt, input, idx, ncpus ->
            tuple(test, fmt, rep, ncpus, 2, input, idx)
        }

    BLOCKY_BCFTOOLS(ch_blocky_t1.mix(ch_blocky_t2))

    // --- Scatter: VCF only, always n=4, compare index types ---
    def index_types = ['csi', 'tbi', 'gzi', 'none']

    ch_scatter = channel.of(index_types).flatten()
        .combine(channel.of(1..params.nreps).flatten())
        .combine(indexed.map { vcf, tbi, csi, gzi, _bcf, _bcf_csi ->
            tuple(vcf, tbi, csi, gzi)
        })
        // -> [index_type, rep, vcf, tbi, csi, gzi]

    BLOCKY_SCATTER(ch_scatter)

    // Collect all results
    ch_results = BCFTOOLS_BASELINE.out.results
        .mix(BLOCKY_BCFTOOLS.out.results)
        .mix(BLOCKY_SCATTER.out.results)

    emit:
    results = ch_results
}
