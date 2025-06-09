import cc1101
import datetime

with cc1101.CC1101() as transceiver:
    # transceiver.set_base_frequency_hertz(433.92e6)
    transceiver.set_base_frequency_hertz(315.00e6)
    transceiver.set_modulation_format(cc1101.ModulationFormat.FSK2)
    # 设置波特率（1kbps）
    transceiver.set_symbol_rate_baud(1000)
    
    # 设置数据包长度（如 4 bytes）
    transceiver.set_packet_length_mode(cc1101.PacketLengthMode.FIXED)
    transceiver.set_packet_length_bytes(4)
    
    print(transceiver)
    
    packet = transceiver._wait_for_packet(
        timeout=datetime.timedelta(seconds=10),
        gdo0_gpio_line_name = b"GPIO25"
    )
    if not packet:
        print("No packet received within the timeout period.")
    else:
        print(f"Received packet: {packet}")
        # Here you can process the packet further, e.g., decode it or extract data.


# def _receive_measurement(
#         self, timeout_seconds: int
#     ) -> typing.Optional[Measurement]:
#         # pylint: disable=protected-access; version pinned
        
#         if not packet:
#             _LOGGER.debug("timeout or fetching packet failed")
#             return None
#         signal = numpy.frombuffer(self._SYNC_WORD + packet.payload, dtype=numpy.uint8)
#         if signal.shape != (self._transmission_length_bytes,):
#             raise _UnexpectedPacketLengthError()
#         try:
#             return self._parse_transmission(signal)
#         except DecodeError as exc:
#             _LOGGER.debug("failed to decode %s: %s", packet, str(exc), exc_info=exc)
#         return None

#     _LOCK_WAIT_START_SECONDS = 2
#     _LOCK_WAIT_FACTOR = 2