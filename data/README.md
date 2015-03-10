# `data.or.kr` 에서 조달청 데이터를 가져옵니다.


## 계약정보: "나라장터 계약현황 규격별 용역 목록"

contract-method:

```
| 1 | 일반경쟁 |
| 2 | 제한경쟁 |
| 3 | 지명경쟁 |
| 4 | 수의계약 |
```


사용예

```
$ python getStndrdAcctoClCntrctInfoListServcCntrctSttus.py --service-key="<서비스 키>" --start-date=20150101 --contract-method=4 --page=1 --number-of-result=10000
```

2015년 1월 1일(`20150101`)부터 현재까지 계약종류 중 *수의계약* 을 한번에 `10000`개씩 가져옵니다. 가져온 데이터는 같은 디렉토리에 `csv` 파일로 저장됩니다.


