set(src
  fwclp.c
  fwclp_conn.c
  fwclp_prot.c
  fwclp_config.c)

FLB_PLUGIN(in_fwclp "${src}" "")
