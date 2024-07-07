package proj.rxjava.fortune;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;

public class FortuneStreamClient {
	
	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("Program require IP Address and Port");
			return;
		}
		int port = 8000;
		try {
			port = Integer.parseInt(args[1]);
		}
		catch(NumberFormatException nfe) {
			System.out.println("Incorrect port");
			System.exit(0);
		}
		Observable<FortuneData> fortuneObs1 = FortuneStream.from(args[0], port);
		partie1Question1(fortuneObs1);
		// partie1Question2(fortuneObs1);
		// partie1Question3(fortuneObs1);
		// partie2Question1(fortuneObs1);
		// partie2Question2(fortuneObs1);
		// partie2Question3(fortuneObs1);
		// partie2Question4(fortuneObs1);
		// partie3Question1(fortuneObs1);
		
		// Observable<FortuneData> fortuneObs2 = FortuneStream.from(args[0], 20000);
		// partie3Question2(fortuneObs1, fortuneObs2);
		
		/* Observable<FortuneData> fortuneObs1 = FortuneStream.from(args[0], 20000)
				.subscribeOn(Schedulers.io())
				.observeOn(Schedulers.newThread());
		
		Observable<FortuneData> fortuneObs2 = FortuneStream.from(args[0], 20000)
				.subscribeOn(Schedulers.computation())
				.observeOn(Schedulers.newThread()); */
		// partie3Question2(fortuneObs1, fortuneObs2);
		
		// partie3Question3(fortuneObs1, fortuneObs2);
	}

	// PARTIE 1
	
	private static void partie1Question1(Observable<FortuneData> fortuneObs1) {
		Disposable disposable = fortuneObs1
				//.subscribeOn(Schedulers.io())
				//.observeOn(Schedulers.io())
				.subscribe(
						ft -> System.out.println(ft), 
						err -> System.out.println("error:" + err)
				);
		stopStreamByHitKey(Collections.singletonList(disposable));
	} 
	
	private static void partie1Question2(Observable<FortuneData> fortuneObs1) {
		Disposable disposable = fortuneObs1
				.subscribeOn(Schedulers.io())
				.observeOn(Schedulers.io())
				.filter(fd -> fd.getDate().getSeconds() % 2 == 0)
				.subscribe(
						ft -> System.out.println(ft), 
						err -> System.out.println("error:" + err)
				);
		stopStreamByHitKey(Collections.singletonList(disposable));
	}
	
	private static void partie1Question3(Observable<FortuneData> fortuneObs1) {
		Disposable disposable = fortuneObs1
				.subscribeOn(Schedulers.io())
				.observeOn(Schedulers.io())
				.filter(fd -> fd.getDate().getSeconds() % 2 == 0)
				.filter(fd -> fd.getText().split("\n").length <= 3)
				.subscribe(
						ft -> System.out.println(ft), 
						err -> System.out.println("error:" + err)
				);
		stopStreamByHitKey(Collections.singletonList(disposable));
	}
	
	// PARTIE 2
	private static void partie2Question1(Observable<FortuneData> fortuneObs1) {
		Disposable disposable = fortuneObs1
				.subscribeOn(Schedulers.io())
				.observeOn(Schedulers.io())
				.map(fd -> fd.getText())
				.map(s -> s.length())
				.subscribe(
						size -> System.out.println(size), 
						err -> System.out.println("error:" + err)
				);
		stopStreamByHitKey(Collections.singletonList(disposable));
	}
	
	private static void partie2Question2(Observable<FortuneData> fortuneObs1) {
		List<String> words = Arrays.asList("is", "the");
		Disposable disposable = fortuneObs1
				.subscribeOn(Schedulers.io())
				.observeOn(Schedulers.io())
				.map(fd -> Arrays.asList(fd.getText().split(" ")))
				.filter(ws -> {
					for(String w:ws) {
						if (words.contains(w)) {
							return true;
						}
					}
					return false;
				})
				.subscribe(
						ws -> System.out.println(ws), 
						err -> System.out.println("error:" + err)
				);
		stopStreamByHitKey(Collections.singletonList(disposable));			
	}
	
	private static void partie2Question3(Observable<FortuneData> fortuneObs1) {
		Map<String, Integer> frequencies = new HashMap<String, Integer>();
		Disposable disposable = fortuneObs1
				.subscribeOn(Schedulers.io())
				.observeOn(Schedulers.io())
				.flatMap(fd -> Observable.fromArray(fd.getText().split(" |\n")))
				.doOnNext(word -> {
					String cleanWord = word.trim().toLowerCase();
					if (cleanWord.length() != 0) {
						frequencies.put(cleanWord, frequencies.getOrDefault(cleanWord, 0) + 1);
					}
				})
				.forEach(word -> {
					if (frequencies.size() % 20 == 0) {
						System.out.println("Current Frequencies:");
						System.out.println(frequencies);
					}
				});
		stopStreamByHitKey(Collections.singletonList(disposable));			
	}
	
	private static void partie2Question4(Observable<FortuneData> fortuneObs1) {
		Map<Date, List<String>> fortune3ByDate = new HashMap<Date, List<String>>();
		Disposable disposable = fortuneObs1
				.subscribeOn(Schedulers.io())
				.observeOn(Schedulers.io())
				.map(fd -> fd.getText())
				.buffer(3)
				.doOnNext(texts -> {
					fortune3ByDate.put(new Date(), texts);
					System.out.println("put");
				})
				.subscribe();
		stopStreamByHitKey(Collections.singletonList(disposable));
		System.out.println("Map:" + fortune3ByDate);
	}
	
	// PARTIE 3
	
	private static void partie3Question1(Observable<FortuneData> fortuneObs1) {
		Disposable disposable = fortuneObs1
				.subscribeOn(Schedulers.io())
				.observeOn(Schedulers.io())
				.subscribe(
						ft -> System.out.println(ft), 
						err -> System.out.println("error:" + err)
				);
		
		Disposable disposable2 = Observable.timer(14, TimeUnit.SECONDS)
			.observeOn(Schedulers.newThread())
			.subscribe(i -> {
				System.out.println("Stop fortune stream");
				disposable.dispose();
			});
				
		stopStreamByHitKey(Arrays.asList(disposable, disposable2));

	}

	private static void partie3Question2(Observable<FortuneData> fortuneObs1, 
			Observable<FortuneData> fortuneObs2) {
		Disposable disposable = Observable.merge(fortuneObs1, fortuneObs2)
				.subscribe(fd -> System.out.println(fd));
		stopStreamByHitKey(Collections.singletonList(disposable));
	}
	
	private static void partie3Question3(Observable<FortuneData> fortuneObs1, 
			Observable<FortuneData> fortuneObs2) {
		Observable<Map.Entry<Integer, String>> entryFortuneObs1 = fortuneObs1
				.map(fd -> new AbstractMap.SimpleEntry(1, fd.getText()));
		
		Observable<Map.Entry<Integer, String>> entryFortuneObs2 = fortuneObs2
				.map(fd -> new AbstractMap.SimpleEntry(2, fd.getText()));
		
		Disposable disposable = Observable.merge(entryFortuneObs1, entryFortuneObs2)
				.doOnNext(entry -> System.out.println("Serveur:" + entry.getKey() + "-" + entry.getValue()))
				.subscribe();
		stopStreamByHitKey(Collections.singletonList(disposable));
	}
	
	protected static void stopStreamByHitKey(List<Disposable> disposables) {
		System.out.println("Hit key to stop fortune client");
		try {
			System.in.read();
			disposables.stream().forEach(d -> d.dispose());
			System.out.println("Fortune stream terminated");
		}
		catch(IOException e) {
			e.printStackTrace();
		}
	}

}
