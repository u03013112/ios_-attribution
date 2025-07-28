import numpy as np
import matplotlib.pyplot as plt

def analyze_cu8_signal_strength(filename, sample_rate=2500000):
    # Read cu8 file
    with open(filename, 'rb') as f:
        data = f.read()
    
    # Convert to IQ data
    raw_data = np.frombuffer(data, dtype=np.uint8)
    I = raw_data[0::2].astype(np.float32)
    Q = raw_data[1::2].astype(np.float32)
    
    # DC correction (127.5 is the midpoint of 8-bit unsigned)
    I = (I - 127.5) / 127.5
    Q = (Q - 127.5) / 127.5
    
    # Calculate complex signal
    signal = I + 1j*Q
    
    # Calculate power (|signal|²)
    power = np.abs(signal)**2
    
    # Convert to dB
    power_db = 10 * np.log10(power + 1e-10)  # Avoid log(0)
    
    # Statistical analysis
    print(f"Signal Strength Statistics:")
    print(f"Average Power: {np.mean(power_db):.2f} dB")
    print(f"Maximum Power: {np.max(power_db):.2f} dB")
    print(f"Minimum Power: {np.min(power_db):.2f} dB")
    print(f"Standard Deviation: {np.std(power_db):.2f} dB")
    
    # Estimate noise floor
    noise_floor = np.percentile(power_db, 10)  # Lowest 10% as noise floor
    signal_peak = np.percentile(power_db, 90)  # Highest 10% as signal peak
    
    print(f"\nEstimated Values:")
    print(f"Noise Floor: {noise_floor:.2f} dB")
    print(f"Signal Peak: {signal_peak:.2f} dB")
    print(f"SNR: {signal_peak - noise_floor:.2f} dB")
    
    # Plot power distribution histogram
    plt.figure(figsize=(12, 4))
    
    plt.subplot(1, 2, 1)
    plt.hist(power_db, bins=100, alpha=0.7)
    plt.axvline(noise_floor, color='red', linestyle='--', label=f'Noise Floor: {noise_floor:.1f}dB')
    plt.axvline(signal_peak, color='green', linestyle='--', label=f'Signal Peak: {signal_peak:.1f}dB')
    plt.xlabel('Power (dB)')
    plt.ylabel('Sample Count')
    plt.title('Power Distribution Histogram')
    plt.legend()
    
    # Plot time-domain power
    plt.subplot(1, 2, 2)
    time_axis = np.arange(len(power_db)) / sample_rate
    plt.plot(time_axis[:10000], power_db[:10000])  # Show only first 10000 points
    plt.xlabel('Time (seconds)')
    plt.ylabel('Power (dB)')
    plt.title('Time-Domain Power Variation')
    
    plt.tight_layout()
    plt.show()
    
    return {
        'mean_power': np.mean(power_db),
        'noise_floor': noise_floor,
        'signal_peak': signal_peak,
        'snr': signal_peak - noise_floor
    }


if __name__ == "__main__":
    # 必须有一个参数，参数就是文件名
    import sys
    if len(sys.argv) != 2:
        print("Usage: python analyze_cu8.py <filename>")
        sys.exit(1)
    filename = sys.argv[1]
    # 使用示例
    result = analyze_cu8_signal_strength(filename)
    # print(f"分析结果: {result}")