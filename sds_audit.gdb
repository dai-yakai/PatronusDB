# sds_audit_ultra.gdb
set pagination off

# 定义单个 SDS 的审计逻辑
define do_audit_logic
    set $s_ptr = (char*)$arg0
    if $s_ptr != 0
        set $sds_count++
        # 读取 flags (s[-1])
        set $flags = *(unsigned char*)($s_ptr - 1)
        set $type = $flags & 7
        
        # 匹配柔性数组布局的偏移量 (Header 紧贴 Data)
        if $type == 0
            # TYPE_5: 高5位是长度
            set $total_len += ($flags >> 3)
            set $total_alloc += ($flags >> 3)
        else if $type == 1
            # TYPE_8: hdr_size = 3 (len:1, alloc:1, flags:1)
            set $total_len += *(unsigned char*)($s_ptr - 3)
            set $total_alloc += *(unsigned char*)($s_ptr - 2)
        else if $type == 2
            # TYPE_16: hdr_size = 5 (len:2, alloc:2, flags:1)
            set $total_len += *(unsigned short*)($s_ptr - 5)
            set $total_alloc += *(unsigned short*)($s_ptr - 3)
        else if $type == 3
            # TYPE_32: hdr_size = 9 (len:4, alloc:4, flags:1)
            set $total_len += *(unsigned int*)($s_ptr - 9)
            set $total_alloc += *(unsigned int*)($s_ptr - 5)
        else if $type == 4
            # TYPE_64: hdr_size = 17 (len:8, alloc:8, flags:1)
            set $total_len += *(unsigned long long*)($s_ptr - 17)
            set $total_alloc += *(unsigned long long*)($s_ptr - 9)
        end
    end
end

define audit_sds
    set $total_alloc = 0
    set $total_len = 0
    set $active_conns = 0
    set $sds_count = 0
    
    # 物理配置信息
    set $base_addr = (char*)0x55a4f415d260
    set $struct_size = 152
    set $rb_offset = 24
    set $wb_offset = 48
    
    # 扫描量，可根据需要调整为 1000000
    set $max = 100
    set $i = 0
    
    printf "Starting Ultra Audit at %p, scanning %d slots...\n", $base_addr, $max

    while $i < $max
        # 1. 物理读取 fd (假设 fd 在结构体开头偏移 0)
        set $curr_ptr = $base_addr + ($i * $struct_size)
        set $fd = *(int*)($curr_ptr + 0)
        
        if $fd > 0
            set $active_conns++
            
            # 2. 读取 read_buffer 指针内容 (偏移 24)
            set $rb = *(char**)($curr_ptr + $rb_offset)
            if $rb != 0
                do_audit_logic $rb
            end
            
            # 3. 读取 write_buffer 指针内容 (偏移 48)
            set $wb = *(char**)($curr_ptr + $wb_offset)
            if $wb != 0
                do_audit_logic $wb
            end
        end
        
        set $i++
        if ($i % 100000 == 0)
            printf "Progress: %d / %d slots scanned\n", $i, $max
        end
    end

    printf "\n================ PatronusDB SDS Final Report ================\n"
    printf "Active Connections: %d\n", $active_conns
    printf "Total SDS Objects:  %d\n", $sds_count
    printf "-------------------------------------------------------------\n"
    printf "Total Allocated:    %.2f MB (%lu bytes)\n", (float)$total_alloc / 1048576, $total_alloc
    printf "Total Used (len):   %.2f MB (%lu bytes)\n", (float)$total_len / 1048576, $total_len
    
    if $total_alloc > 0
        set $waste = $total_alloc - $total_len
        printf "Wasted Memory:      %.2f MB (%.2f%%)\n", (float)$waste / 1048576, (float)$waste * 100 / $total_alloc
    end
    printf "=============================================================\n"
end

# 开始运行
audit_sds