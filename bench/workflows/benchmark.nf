include { PREPARE_INPUT }   from '../subworkflows/local/prepare_input'
include { BENCHMARK_TESTS } from '../subworkflows/local/benchmark_tests'
include { REPORT }          from '../modules/local/report'

workflow BENCHMARK {
    // Input channels
    ch_test_vcf    = Channel.fromPath(params.test_vcf, checkIfExists: true)
    ch_clinvar_vcf = Channel.fromPath(params.clinvar_vcf, checkIfExists: true)

    // Prepare indices and vcfanno.conf
    PREPARE_INPUT(ch_test_vcf, ch_clinvar_vcf)

    // Run all benchmarks
    BENCHMARK_TESTS(PREPARE_INPUT.out.indexed, PREPARE_INPUT.out.vcfanno_bundle)

    // Collect all results into single TSV
    ch_data = BENCHMARK_TESTS.out.results
        .collectFile(name: 'benchmark_data.tsv', storeDir: params.outdir, keepHeader: true)

    // Generate report
    ch_rmd = channel.fromPath("${projectDir}/assets/report.Rmd", checkIfExists: true)
    REPORT(ch_data, ch_rmd)
}
