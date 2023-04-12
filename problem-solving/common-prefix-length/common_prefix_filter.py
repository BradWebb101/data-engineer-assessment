def commonPrefix(inputs:list[str]) -> list[int]:
    output_list = []
    for s in inputs:
        results = []
        common_prefix = 0
        for i in range(len(s)):
            sub_results = []
            suffix = s[i:]
            for t in reversed(range(len(suffix))):
                if suffix[:t+1] == s[:t+1]:
                    results.append(len(suffix[:t+1]))
                    break
      
        output_list.append(sum(results))
        
    return output_list


if __name__ == '__main__':
    test = commonPrefix(['abcdef', 'fedcba'])
    print(test)