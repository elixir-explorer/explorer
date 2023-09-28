defmodule Explorer.ComptimeUtilsTest do
  use ExUnit.Case, async: true
  alias Explorer.ComptimeUtils

  describe "cpu_with_all_caps?/2" do
    @contents """
    processor	: 31
    model		: 33
    model name	: AMD Ryzen 9 5950X 16-Core Processor
    cpuid level	: 16
    wp		: yes
    flags		: fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ht syscall nx mmxext fxsr_opt pdpe1gb rdtscp lm constant_tsc rep_good nopl nonstop_tsc cpuid extd_apicid aperfmperf rapl pni pclmulqdq monitor ssse3 fma cx16 sse4_1 sse4_2 x2apic movbe popcnt aes xsave avx f16c rdrand lahf_lm cmp_legacy svm extapic cr8_legacy abm sse4a misalignsse 3dnowprefetch osvw ibs skinit wdt tce topoext perfctr_core perfctr_nb bpext perfctr_llc mwaitx cpb cat_l3 cdp_l3 hw_pstate ssbd mba ibrs ibpb stibp vmmcall fsgsbase bmi1 avx2 smep bmi2 erms invpcid cqm rdt_a rdseed adx smap clflushopt clwb sha_ni xsaveopt xsavec xgetbv1 xsaves cqm_llc cqm_occup_llc cqm_mbm_total cqm_mbm_local clzero irperf xsaveerptr rdpru wbnoinvd arat npt lbrv svm_lock nrip_save tsc_scale vmcb_clean flushbyasid decodeassists pausefilter pfthreshold avic v_vmsave_vmload vgif v_spec_ctrl umip pku ospke vaes vpclmulqdq rdpid overflow_recov succor smca fsrm
    bugs		: sysret_ss_attrs spectre_v1 spectre_v2 spec_store_bypass srso
    bogomips	: 6786.91

    processor	: 31
    model		: 33
    model name	: AMD Ryzen 9 5950X 16-Core Processor
    cpuid level	: 16
    wp		: yes
    flags		: fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ht syscall nx mmxext fxsr_opt pdpe1gb rdtscp lm constant_tsc rep_good nopl nonstop_tsc cpuid extd_apicid aperfmperf rapl pni pclmulqdq monitor ssse3 fma cx16 sse4_1 sse4_2 x2apic movbe popcnt aes xsave avx f16c rdrand lahf_lm cmp_legacy svm extapic cr8_legacy abm sse4a misalignsse 3dnowprefetch osvw ibs skinit wdt tce topoext perfctr_core perfctr_nb bpext perfctr_llc mwaitx cpb cat_l3 cdp_l3 hw_pstate ssbd mba ibrs ibpb stibp vmmcall fsgsbase bmi1 avx2 smep bmi2 erms invpcid cqm rdt_a rdseed adx smap clflushopt clwb sha_ni xsaveopt xsavec xgetbv1 xsaves cqm_llc cqm_occup_llc cqm_mbm_total cqm_mbm_local clzero irperf xsaveerptr rdpru wbnoinvd arat npt lbrv svm_lock nrip_save tsc_scale vmcb_clean flushbyasid decodeassists pausefilter pfthreshold avic v_vmsave_vmload vgif v_spec_ctrl umip pku ospke vaes vpclmulqdq rdpid overflow_recov succor smca fsrm
    bugs		: sysret_ss_attrs spectre_v1 spectre_v2 spec_store_bypass srso
    bogomips	: 6786.91

    """

    @tag :tmp_dir
    test "detects capabilities when CPU supports all of them", %{tmp_dir: tmp_dir} do
      path = Path.join([tmp_dir, "with_avx"])
      File.write!(path, @contents)

      assert ComptimeUtils.cpu_with_all_caps?(~w[sse avx avx2], cpu_info_file_path: path)
    end

    @tag :tmp_dir
    test "does not detect capabilities when CPU is missing one of them", %{tmp_dir: tmp_dir} do
      path = Path.join([tmp_dir, "with_avx"])
      File.write!(path, @contents)

      refute ComptimeUtils.cpu_with_all_caps?(~w[sse avx avx2 avx26], cpu_info_file_path: path)
    end

    @tag :tmp_dir
    test "does not detect capabilities when file is missing", %{tmp_dir: tmp_dir} do
      path = Path.join([tmp_dir, "with_avx"])

      refute ComptimeUtils.cpu_with_all_caps?(~w[avx], cpu_info_file_path: path)
    end
  end
end
