class Solution {
    class AnaGroup {
        HashMap<Character, Integer> hashTable;
        List<String> words;

        AnaGroup() {};
        AnaGroup(HashMap<Character, Integer> hs, List<String> w) {
            this.hashTable = hs;
            this.words = w;
        }
    }

    public List<List<String>> groupAnagrams(String[] strs) {
        List<AnaGroup> anagramGroups = new ArrayList<>();

        for (String st : strs) {
            HashMap<Character, Integer> stTable = new HashMap<>();
            for (char c : st.toCharArray()) {
                if(stTable.containsKey(c)) {
                    stTable.put(c, stTable.get(c) + 1);
                } else {
                    stTable.put(c, 1);
                }
            }
            for (AnaGroup anagramGroup : anagramGroups) {
                if (anagramGroup.hashTable.equals(stTable)) {
                    anagramGroup.words.add(st);
                    continue;
                }
            }
            AnaGroup toAdd = new AnaGroup(stTable, List.of(st));
            anagramGroups.add(toAdd);
        }

        List<List<String>> ans = new ArrayList<>();
        for (AnaGroup a : anagramGroups) {
            ans.add(a.words);
        }

        return ans;
    }


    private void test() {
        // get value from map and append to list
        HashMap<Character, List<Integer>> map = new HashMap<>();
        map.put('a', List.of(1, 2, 3));
        map.put('b', List.of(4, 5, 6));
        map.put('c', List.of(7, 8, 9));
        map.computeIfAbsent('d', k -> new ArrayList<>()).add(10);

        HashMap<Integer, Integer> map2 = new HashMap<>();
        map2.put(1, 1);
        map2.put(2, 2);
        map2.put(3, 3);
        map2.merge(4, 1, Integer::sum);
        map2.compute(4, (k, v) -> v == null ? 1 : v + 1);
    }
}


