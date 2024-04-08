import pinyin from "pinyin";

console.log(pinyin("中心"));    // [ [ 'zhōng' ], [ 'xīn' ] ]

console.log(pinyin("中心", {
  heteronym: true,              // 启用多音字模式
}));                            // [ [ 'zhōng', 'zhòng' ], [ 'xīn' ] ]

console.log(pinyin("中心", {
  heteronym: true,              // 启用多音字模式
  segment: true,                // 启用分词，以解决多音字问题。默认不开启，使用 true 开启使用 Intl.Segmenter 分词库。
}));                            // [ [ 'zhōng' ], [ 'xīn' ] ]

console.log(pinyin("中心", {
  segment: "@node-rs/jieba",    // 指定分词库，可以是 "Intl.Segmenter", "nodejieba"、"segmentit"、"@node-rs/jieba"。
}));                            // [ [ 'zhōng' ], [ 'xīn' ] ]

console.log(pinyin("我喜欢你", {
  segment: "segmentit",         // 启用分词
  group: true,                  // 启用词组
}));                            // [ [ 'wǒ' ], [ 'xǐhuān' ], [ 'nǐ' ] ]

console.log(pinyin("中心", {
  style: "initials",            // 设置拼音风格。
  heteronym: true,              // 即使有多音字，因为拼音风格选择，重复的也会合并。
}));                            // [ [ 'zh' ], [ 'x' ] ]

console.log(pinyin("华夫人", {
  mode: "surname",              // 姓名模式。
}));                            // [ ['huà'], ['fū'], ['rén'] ]