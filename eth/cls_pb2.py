# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: cls.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\tcls.proto\"d\n\x0bStreamField\x12\x11\n\ttimeStamp\x18\x01 \x01(\x12\x12\r\n\x05\x66rom_\x18\x02 \x01(\t\x12\n\n\x02to\x18\x03 \x01(\t\x12\x11\n\tspaceName\x18\x04 \x01(\t\x12\x14\n\x0cspecialField\x18\x05 \x01(\x0c\"\x92\x11\n\x04Node\x12\x0f\n\x07\x61\x64\x64ress\x18\x01 \x01(\t\x12\x1a\n\x12total_transactions\x18\x02 \x01(\x12\x12\"\n\x1anumber_of_received_address\x18\x03 \x01(\x05\x12\x1e\n\x16number_of_sent_address\x18\x04 \x01(\x05\x12\x19\n\x11\x63reated_contracts\x18\x05 \x01(\x05\x12\x1a\n\x12min_value_received\x18\x06 \x01(\x01\x12\x1a\n\x12max_value_received\x18\x07 \x01(\x01\x12\x1a\n\x12\x61vg_value_received\x18\x08 \x01(\x01\x12%\n\x1dtotal_ether_sent_for_accounts\x18\t \x01(\x01\x12\x16\n\x0emin_value_sent\x18\n \x01(\x01\x12\x16\n\x0emax_value_sent\x18\x0b \x01(\x01\x12\x16\n\x0e\x61vg_value_sent\x18\x0c \x01(\x01\x12)\n!total_ether_received_for_accounts\x18\r \x01(\x01\x12#\n\x1bmin_value_sent_to_contracts\x18\x0e \x01(\x01\x12#\n\x1bmax_value_sent_to_contracts\x18\x0f \x01(\x01\x12#\n\x1b\x61vg_value_sent_to_contracts\x18\x10 \x01(\x01\x12&\n\x1etotal_ether_sent_for_contracts\x18\x11 \x01(\x01\x12\x1d\n\x15USDT_sent_to_contract\x18\x12 \x01(\x01\x12\x1c\n\x14\x42NB_sent_to_contract\x18\x13 \x01(\x01\x12\x1c\n\x14UNI_sent_to_contract\x18\x14 \x01(\x01\x12\x1d\n\x15USDC_sent_to_contract\x18\x15 \x01(\x01\x12\x1d\n\x15LINK_sent_to_contract\x18\x16 \x01(\x01\x12\x1e\n\x16MATIC_sent_to_contract\x18\x17 \x01(\x01\x12\x1c\n\x14HEX_sent_to_contract\x18\x18 \x01(\x01\x12\x1d\n\x15\x42USD_sent_to_contract\x18\x19 \x01(\x01\x12\x1d\n\x15WBTC_sent_to_contract\x18\x1a \x01(\x01\x12\x1c\n\x14\x41\x43\x41_sent_to_contract\x18\x1b \x01(\x01\x12\x1d\n\x15\x41\x41VE_sent_to_contract\x18\x1c \x01(\x01\x12\x1c\n\x14SAI_sent_to_contract\x18\x1d \x01(\x01\x12\x1c\n\x14\x44\x41I_sent_to_contract\x18\x1e \x01(\x01\x12\x1d\n\x15SHIB_sent_to_contract\x18\x1f \x01(\x01\x12\x1b\n\x13HT_sent_to_contract\x18  \x01(\x01\x12\x1c\n\x14MKR_sent_to_contract\x18! \x01(\x01\x12\x1c\n\x14\x43RO_sent_to_contract\x18\" \x01(\x01\x12\x1d\n\x15\x43OMP_sent_to_contract\x18# \x01(\x01\x12\x1c\n\x14LEO_sent_to_contract\x18$ \x01(\x01\x12\x1c\n\x14\x46\x45I_sent_to_contract\x18% \x01(\x01\x12\x11\n\tUSDT_sent\x18& \x01(\x01\x12\x10\n\x08\x42NB_sent\x18\' \x01(\x01\x12\x10\n\x08UNI_sent\x18( \x01(\x01\x12\x11\n\tUSDC_sent\x18) \x01(\x01\x12\x11\n\tLINK_sent\x18* \x01(\x01\x12\x12\n\nMATIC_sent\x18+ \x01(\x01\x12\x10\n\x08HEX_sent\x18, \x01(\x01\x12\x11\n\tBUSD_sent\x18- \x01(\x01\x12\x11\n\tWBTC_sent\x18. \x01(\x01\x12\x10\n\x08\x41\x43\x41_sent\x18/ \x01(\x01\x12\x11\n\tAAVE_sent\x18\x30 \x01(\x01\x12\x10\n\x08SAI_sent\x18\x31 \x01(\x01\x12\x10\n\x08\x44\x41I_sent\x18\x32 \x01(\x01\x12\x11\n\tSHIB_sent\x18\x33 \x01(\x01\x12\x0f\n\x07HT_sent\x18\x34 \x01(\x01\x12\x10\n\x08MKR_sent\x18\x35 \x01(\x01\x12\x10\n\x08\x43RO_sent\x18\x36 \x01(\x01\x12\x11\n\tCOMP_sent\x18\x37 \x01(\x01\x12\x10\n\x08LEO_sent\x18\x38 \x01(\x01\x12\x10\n\x08\x46\x45I_sent\x18\x39 \x01(\x01\x12\x15\n\rUSDT_received\x18: \x01(\x01\x12\x14\n\x0c\x42NB_received\x18; \x01(\x01\x12\x14\n\x0cUNI_received\x18< \x01(\x01\x12\x15\n\rUSDC_received\x18= \x01(\x01\x12\x15\n\rLINK_received\x18> \x01(\x01\x12\x16\n\x0eMATIC_received\x18? \x01(\x01\x12\x14\n\x0cHEX_received\x18@ \x01(\x01\x12\x15\n\rBUSD_received\x18\x41 \x01(\x01\x12\x15\n\rWBTC_received\x18\x42 \x01(\x01\x12\x14\n\x0c\x41\x43\x41_received\x18\x43 \x01(\x01\x12\x15\n\rAAVE_received\x18\x44 \x01(\x01\x12\x14\n\x0cSAI_received\x18\x45 \x01(\x01\x12\x14\n\x0c\x44\x41I_received\x18\x46 \x01(\x01\x12\x15\n\rSHIB_received\x18G \x01(\x01\x12\x13\n\x0bHT_received\x18H \x01(\x01\x12\x14\n\x0cMKR_received\x18I \x01(\x01\x12\x14\n\x0c\x43RO_received\x18J \x01(\x01\x12\x15\n\rCOMP_received\x18K \x01(\x01\x12\x14\n\x0cLEO_received\x18L \x01(\x01\x12\x14\n\x0c\x46\x45I_received\x18M \x01(\x01\x12\x1d\n\x15\x61vg_time_between_sent\x18N \x01(\x12\x12!\n\x19\x61vg_time_between_received\x18O \x01(\x12\x12\x1f\n\x17time_between_first_last\x18P \x01(\x12\x12\x1b\n\x13\x45RC20_avg_time_sent\x18Q \x01(\x12\x12\x1f\n\x17\x45RC20_avg_time_received\x18R \x01(\x12\x12 \n\x18\x45RC20_avg_time_contracts\x18S \x01(\x12\x12\x10\n\x08indegree\x18T \x01(\x12\x12\x11\n\toutdegree\x18U \x01(\x12\"\xa4\x02\n\x04\x45\x64ge\x12\x11\n\ttimestamp\x18\x01 \x01(\x12\x12\r\n\x05_from\x18\x02 \x01(\t\x12\n\n\x02to\x18\x03 \x01(\t\x12\x0c\n\x04\x63oin\x18\x04 \x01(\t\x12\r\n\x05value\x18\x05 \x01(\x01\x12\x11\n\ttransHash\x18\x06 \x01(\t\x12\x0f\n\x07gasUsed\x18\x07 \x01(\x01\x12\x10\n\x08gasLimit\x18\x08 \x01(\x01\x12\x0b\n\x03\x66\x65\x65\x18\t \x01(\x01\x12\x10\n\x08\x66romType\x18\n \x01(\t\x12\x0e\n\x06toType\x18\x0b \x01(\t\x12\x11\n\ttransType\x18\x0c \x01(\t\x12\x0e\n\x06isLoop\x18\r \x01(\x05\x12\x0e\n\x06status\x18\x0e \x01(\x05\x12\n\n\x02id\x18\x0f \x01(\x03\x12\x0c\n\x04rank\x18\x10 \x01(\x03\x12\x0c\n\x04rate\x18\x11 \x01(\x01\x12\x11\n\tusd_price\x18\x12 \x01(\x01\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'cls_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _globals['_STREAMFIELD']._serialized_start=13
  _globals['_STREAMFIELD']._serialized_end=113
  _globals['_NODE']._serialized_start=116
  _globals['_NODE']._serialized_end=2310
  _globals['_EDGE']._serialized_start=2313
  _globals['_EDGE']._serialized_end=2605
# @@protoc_insertion_point(module_scope)
