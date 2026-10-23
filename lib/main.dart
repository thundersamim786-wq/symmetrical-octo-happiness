import 'dart:async';
import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:just_audio/just_audio.dart';
import 'package:just_audio_background/just_audio_background.dart';
import 'package:rxdart/rxdart.dart';
import 'package:share_plus/share_plus.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:youtube_explode_dart/youtube_explode_dart.dart';

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await JustAudioBackground.init(
    androidNotificationChannelId: 'com.spotube.edu.channel.audio',
    androidNotificationChannelName: 'Audio Playback',
    androidNotificationOngoing: true,
  );
  final prefs = await SharedPreferences.getInstance();
  runApp(MusicStreamApp(prefs: prefs));
}

// ============================================================================
// THEME CONSTANTS
// ============================================================================
class AppColors {
  static const Color background = Color(0xFF121212);
  static const Color cardDark = Color(0xFF181818);
  static const Color cardElevated = Color(0xFF282828);
  static const Color spotifyGreen = Color(0xFF1DB954);
  static const Color textPrimary = Color(0xFFFFFFFF);
  static const Color textSecondary = Color(0xFFB3B3B3);
}

// ============================================================================
// DATA MODELS
// ============================================================================
class TrackModel {
  final String videoId;
  final String title;
  final String author;
  final String thumbnailUrl;
  final Duration duration;

  const TrackModel({
    required this.videoId,
    required this.title,
    required this.author,
    required this.thumbnailUrl,
    required this.duration,
  });

  Map<String, dynamic> toJson() => {
        'v': videoId,
        't': title,
        'a': author,
        'i': thumbnailUrl,
        'd': duration.inSeconds,
      };

  factory TrackModel.fromJson(Map<String, dynamic> json) {
    return TrackModel(
      videoId: json['v'] as String? ?? '',
      title: json['t'] as String? ?? 'Unknown Title',
      author: json['a'] as String? ?? 'Unknown Artist',
      thumbnailUrl: json['i'] as String? ?? '',
      duration: Duration(seconds: (json['d'] as num?)?.toInt() ?? 0),
    );
  }

  factory TrackModel.fromVideo(Video video) {
    return TrackModel(
      videoId: video.id.value,
      title: video.title,
      author: video.author,
      thumbnailUrl: video.thumbnails.highResUrl,
      duration: video.duration ?? Duration.zero,
    );
  }

  String toShareToken() {
    final raw = jsonEncode(toJson());
    return 'spotube://track/${base64Url.encode(utf8.encode(raw))}';
  }

  static TrackModel? fromShareToken(String token) {
    try {
      final cleaned = token.trim().replaceFirst('spotube://track/', '');
      final decoded = utf8.decode(base64Url.decode(cleaned));
      final map = jsonDecode(decoded) as Map<String, dynamic>;
      if ((map['v'] as String?)?.isEmpty ?? true) return null;
      return TrackModel.fromJson(map);
    } catch (_) {
      return null;
    }
  }
}

class PositionData {
  final Duration position;
  final Duration bufferedPosition;
  final Duration duration;

  const PositionData(this.position, this.bufferedPosition, this.duration);
}

// ============================================================================
// AUTHENTICATION CONTROLLER
// ============================================================================
class AuthController extends ChangeNotifier {
  static const String _keyUserEmail = 'auth_user_email';
  static const String _keyIsGuest = 'auth_is_guest';
  static const String _keyAccounts = 'auth_accounts_map';

  final SharedPreferences _prefs;
  String? _email;
  bool _isGuest = false;

  AuthController(this._prefs) {
    _email = _prefs.getString(_keyUserEmail);
    _isGuest = _prefs.getBool(_keyIsGuest) ?? false;
  }

  bool get isAuthenticated => (_email != null && _email!.isNotEmpty) || _isGuest;
  bool get isGuest => _isGuest;
  String get displayName => _isGuest ? 'Guest Listener' : (_email ?? 'User');

  Map<String, String> _getAccounts() {
    final raw = _prefs.getString(_keyAccounts);
    if (raw == null || raw.isEmpty) return {};
    final decoded = jsonDecode(raw) as Map<String, dynamic>;
    return decoded.map((k, v) => MapEntry(k, v.toString()));
  }

  Future<String?> signIn({
    required String email,
    required String password,
    required bool isSignUp,
  }) async {
    final trimmedEmail = email.trim().toLowerCase();
    if (!trimmedEmail.contains('@') || password.length < 4) {
      return 'Enter a valid email and a password of at least 4 characters.';
    }

    final accounts = _getAccounts();
    if (isSignUp) {
      if (accounts.containsKey(trimmedEmail)) {
        return 'An account with this email already exists. Please log in.';
      }
      accounts[trimmedEmail] = password;
      await _prefs.setString(_keyAccounts, jsonEncode(accounts));
    } else {
      if (!accounts.containsKey(trimmedEmail) ||
          accounts[trimmedEmail] != password) {
        return 'Invalid email or password.';
      }
    }

    _email = trimmedEmail;
    _isGuest = false;
    await _prefs.setString(_keyUserEmail, _email!);
    await _prefs.setBool(_keyIsGuest, false);
    notifyListeners();
    return null;
  }

  Future<void> continueAsGuest() async {
    _email = null;
    _isGuest = true;
    await _prefs.remove(_keyUserEmail);
    await _prefs.setBool(_keyIsGuest, true);
    notifyListeners();
  }

  Future<void> signOut() async {
    _email = null;
    _isGuest = false;
    await _prefs.remove(_keyUserEmail);
    await _prefs.setBool(_keyIsGuest, false);
    notifyListeners();
  }
}

// ============================================================================
// AUDIO & YOUTUBE STREAM CONTROLLER
// ============================================================================
class PlayerController extends ChangeNotifier {
  final YoutubeExplode _yt = YoutubeExplode();
  final AudioPlayer _player = AudioPlayer();
  final ConcatenatingAudioSource _playlist =
      ConcatenatingAudioSource(children: []);

  final List<TrackModel> _queue = [];
  final Map<String, Uri> _resolvedUrlCache = {};

  StreamSubscription<int?>? _indexSub;
  StreamSubscription<PlayerState>? _stateSub;
  StreamSubscription<LoopMode>? _loopSub;
  StreamSubscription<bool>? _shuffleSub;

  bool _isResolvingStream = false;
  String? _errorMessage;

  PlayerController() {
    _initPlayer();
  }

  AudioPlayer get player => _player;
  List<TrackModel> get queue => List.unmodifiable(_queue);
  bool get isResolvingStream => _isResolvingStream;
  String? get errorMessage => _errorMessage;

  TrackModel? get currentTrack {
    final idx = _player.currentIndex;
    if (idx != null && idx >= 0 && idx < _queue.length) {
      return _queue[idx];
    }
    return null;
  }

  Stream<PositionData> get positionDataStream =>
      Rx.combineLatest3<Duration, Duration, Duration?, PositionData>(
        _player.positionStream,
        _player.bufferedPositionStream,
        _player.durationStream,
        (position, buffered, duration) =>
            PositionData(position, buffered, duration ?? Duration.zero),
      );

  Future<void> _initPlayer() async {
    await _player.setAudioSource(_playlist);
    _indexSub = _player.currentIndexStream.listen((_) => notifyListeners());
    _stateSub = _player.playerStateStream.listen((_) => notifyListeners());
    _loopSub = _player.loopModeStream.listen((_) => notifyListeners());
    _shuffleSub =
        _player.shuffleModeEnabledStream.listen((_) => notifyListeners());
  }

  void clearError() {
    _errorMessage = null;
    notifyListeners();
  }

  Future<List<TrackModel>> searchYouTube(String query) async {
    final trimmed = query.trim();
    if (trimmed.isEmpty) return [];
    final searchList = await _yt.search.search(trimmed);
    return searchList.map(TrackModel.fromVideo).toList();
  }

  Future<Uri> _resolveAudioUri(String videoId) async {
    if (_resolvedUrlCache.containsKey(videoId)) {
      return _resolvedUrlCache[videoId]!;
    }
    final manifest = await _yt.videos.streamsClient.getManifest(videoId);
    final audioStreams = manifest.audioOnly;
    if (audioStreams.isEmpty) {
      throw StateError('No audio stream available for video $videoId');
    }

    final sorted = audioStreams.sortByBitrate();
    final selected = sorted.first;
    _resolvedUrlCache[videoId] = selected.url;
    return selected.url;
  }

  AudioSource _buildAudioSource(TrackModel track, Uri streamUri) {
    return AudioSource.uri(
      streamUri,
      tag: MediaItem(
        id: track.videoId,
        album: 'YouTube Audio Stream',
        title: track.title,
        artist: track.author,
        duration: track.duration,
        artUri: Uri.tryParse(track.thumbnailUrl),
      ),
    );
  }

  Future<void> playTrack(TrackModel track, {bool playImmediately = true}) async {
    _isResolvingStream = true;
    _errorMessage = null;
    notifyListeners();

    try {
      final existingIndex =
          _queue.indexWhere((element) => element.videoId == track.videoId);
      if (existingIndex != -1) {
        if (playImmediately) {
          await _player.seek(Duration.zero, index: existingIndex);
          unawaited(_player.play());
        }
        return;
      }

      final uri = await _resolveAudioUri(track.videoId);
      final source = _buildAudioSource(track, uri);
      _queue.add(track);
      await _playlist.add(source);
      notifyListeners();

      if (playImmediately) {
        final targetIndex = _queue.length - 1;
        await _player.seek(Duration.zero, index: targetIndex);
        unawaited(_player.play());
      }
    } catch (e) {
      _errorMessage =
          'Unable to play "${track.title}". Stream may be region-locked or restricted.';
    } finally {
      _isResolvingStream = false;
      notifyListeners();
    }
  }

  Future<void> addToQueue(TrackModel track) async {
    await playTrack(track, playImmediately: _queue.isEmpty);
  }

  Future<void> playFromQueueIndex(int index) async {
    if (index < 0 || index >= _queue.length) return;
    await _player.seek(Duration.zero, index: index);
    unawaited(_player.play());
  }

  Future<void> removeFromQueue(int index) async {
    if (index < 0 || index >= _queue.length) return;
    _queue.removeAt(index);
    await _playlist.removeAt(index);
    notifyListeners();
  }

  Future<void> togglePlayPause() async {
    if (_player.playing) {
      await _player.pause();
    } else {
      await _player.play();
    }
  }

  Future<void> toggleShuffle() async {
    final enabled = !_player.shuffleModeEnabled;
    if (enabled) {
      await _player.shuffle();
    }
    await _player.setShuffleModeEnabled(enabled);
  }

  Future<void> cycleLoopMode() async {
    final nextMode = switch (_player.loopMode) {
      LoopMode.off => LoopMode.all,
      LoopMode.all => LoopMode.one,
      LoopMode.one => LoopMode.off,
    };
    await _player.setLoopMode(nextMode);
  }

  Future<void> shareTrack(TrackModel track) async {
    final shareToken = track.toShareToken();
    final message =
        'Listen to "${track.title}" by ${track.author} on Spotube Edu!\n\n'
        'YouTube: https://youtu.be/${track.videoId}\n'
        'Import Code:\n$shareToken';
    await Share.share(message, subject: track.title);
  }

  @override
  void dispose() {
    _indexSub?.cancel();
    _stateSub?.cancel();
    _loopSub?.cancel();
    _shuffleSub?.cancel();
    _player.dispose();
    _yt.close();
    super.dispose();
  }
}

// ============================================================================
// ROOT APPLICATION WIDGET
// ============================================================================
class MusicStreamApp extends StatefulWidget {
  final SharedPreferences prefs;

  const MusicStreamApp({super.key, required this.prefs});

  @override
  State<MusicStreamApp> createState() => _MusicStreamAppState();
}

class _MusicStreamAppState extends State<MusicStreamApp> {
  late final AuthController _authController;
  late final PlayerController _playerController;

  @override
  void initState() {
    super.initState();
    _authController = AuthController(widget.prefs);
    _playerController = PlayerController();
  }

  @override
  void dispose() {
    _authController.dispose();
    _playerController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Spotube Edu',
      debugShowCheckedModeBanner: false,
      theme: ThemeData(
        brightness: Brightness.dark,
        scaffoldBackgroundColor: AppColors.background,
        primaryColor: AppColors.spotifyGreen,
        colorScheme: const ColorScheme.dark(
          primary: AppColors.spotifyGreen,
          secondary: AppColors.spotifyGreen,
          surface: AppColors.cardDark,
        ),
        sliderTheme: SliderThemeData(
          activeTrackColor: AppColors.spotifyGreen,
          inactiveTrackColor: Colors.white24,
          thumbColor: Colors.white,
          overlayColor: AppColors.spotifyGreen.withOpacity(0.2),
          trackHeight: 3.0,
        ),
        snackBarTheme: const SnackBarThemeData(
          backgroundColor: AppColors.cardElevated,
          contentTextStyle: TextStyle(color: AppColors.textPrimary),
        ),
      ),
      home: ListenableBuilder(
        listenable: _authController,
        builder: (context, _) {
          if (!_authController.isAuthenticated) {
            return AuthScreen(authController: _authController);
          }
          return HomeScreen(
            authController: _authController,
            playerController: _playerController,
          );
        },
      ),
    );
  }
}

// ============================================================================
// AUTH SCREEN
// ============================================================================
class AuthScreen extends StatefulWidget {
  final AuthController authController;

  const AuthScreen({super.key, required this.authController});

  @override
  State<AuthScreen> createState() => _AuthScreenState();
}

class _AuthScreenState extends State<AuthScreen> {
  final TextEditingController _emailCtrl = TextEditingController();
  final TextEditingController _passwordCtrl = TextEditingController();
  bool _isSignUp = false;
  String? _error;
  bool _submitting = false;

  @override
  void dispose() {
    _emailCtrl.dispose();
    _passwordCtrl.dispose();
    super.dispose();
  }

  Future<void> _submit() async {
    setState(() {
      _submitting = true;
      _error = null;
    });

    final err = await widget.authController.signIn(
      email: _emailCtrl.text,
      password: _passwordCtrl.text,
      isSignUp: _isSignUp,
    );

    if (mounted) {
      setState(() {
        _submitting = false;
        _error = err;
      });
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      body: SafeArea(
        child: Center(
          child: SingleChildScrollView(
            padding: const EdgeInsets.symmetric(horizontal: 28, vertical: 24),
            child: ConstrainedBox(
              constraints: const BoxConstraints(maxWidth: 400),
              child: Column(
                mainAxisSize: MainAxisSize.min,
                children: [
                  const Icon(Icons.graphic_eq_rounded, size: 76, color: AppColors.spotifyGreen),
                  const SizedBox(height: 16),
                  const Text(
                    'Millions of songs.\nStreamed cleanly.',
                    textAlign: TextAlign.center,
                    style: TextStyle(fontSize: 28, fontWeight: FontWeight.bold, color: AppColors.textPrimary, height: 1.2),
                  ),
                  const SizedBox(height: 32),
                  Container(
                    padding: const EdgeInsets.all(20),
                    decoration: BoxDecoration(
                      color: AppColors.cardDark,
                      borderRadius: BorderRadius.circular(16),
                      border: Border.all(color: Colors.white10),
                    ),
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.stretch,
                      children: [
                        Text(
                          _isSignUp ? 'Create Account' : 'Sign In',
                          style: const TextStyle(fontSize: 20, fontWeight: FontWeight.w700),
                        ),
                        const SizedBox(height: 16),
                        TextField(
                          controller: _emailCtrl,
                          keyboardType: TextInputType.emailAddress,
                          decoration: InputDecoration(
                            labelText: 'Email address',
                            filled: true,
                            fillColor: AppColors.cardElevated,
                            border: OutlineInputBorder(borderRadius: BorderRadius.circular(10), borderSide: BorderSide.none),
                          ),
                        ),
                        const SizedBox(height: 12),
                        TextField(
                          controller: _passwordCtrl,
                          obscureText: true,
                          decoration: InputDecoration(
                            labelText: 'Password',
                            filled: true,
                            fillColor: AppColors.cardElevated,
                            border: OutlineInputBorder(borderRadius: BorderRadius.circular(10), borderSide: BorderSide.none),
                          ),
                        ),
                        if (_error != null) ...[
                          const SizedBox(height: 12),
                          Text(_error!, style: const TextStyle(color: Colors.redAccent, fontSize: 13)),
                        ],
                        const SizedBox(height: 20),
                        ElevatedButton(
                          onPressed: _submitting ? null : _submit,
                          style: ElevatedButton.styleFrom(
                            backgroundColor: AppColors.spotifyGreen,
                            foregroundColor: Colors.black,
                            padding: const EdgeInsets.symmetric(vertical: 15),
                            shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(30)),
                          ),
                          child: Text(
                            _isSignUp ? 'SIGN UP' : 'LOG IN',
                            style: const TextStyle(fontWeight: FontWeight.bold, letterSpacing: 1.1),
                          ),
                        ),
                        const SizedBox(height: 10),
                        TextButton(
                          onPressed: () => setState(() {
                            _isSignUp = !_isSignUp;
                            _error = null;
                          }),
                          child: Text(
                            _isSignUp ? 'Already have an account? Log in' : "Don't have an account? Sign up",
                            style: const TextStyle(color: AppColors.textSecondary),
                          ),
                        ),
                      ],
                    ),
                  ),
                  const SizedBox(height: 20),
                  OutlinedButton.icon(
                    onPressed: widget.authController.continueAsGuest,
                    icon: const Icon(Icons.person_outline),
                    label: const Text('CONTINUE AS GUEST'),
                    style: OutlinedButton.styleFrom(
                      foregroundColor: AppColors.textPrimary,
                      side: const BorderSide(color: Colors.white24),
                      padding: const EdgeInsets.symmetric(horizontal: 24, vertical: 14),
                      shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(30)),
                    ),
                  ),
                ],
              ),
            ),
          ),
        ),
      ),
    );
  }
}
// ============================================================================
// HOME / SEARCH & BROWSE SCREEN
// ============================================================================
class HomeScreen extends StatefulWidget {
  final AuthController authController;
  final PlayerController playerController;

  const HomeScreen({
    super.key,
    required this.authController,
    required this.playerController,
  });

  @override
  State<HomeScreen> createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> {
  final TextEditingController _searchCtrl = TextEditingController();
  Timer? _debounce;
  List<TrackModel> _results = [];
  bool _isSearching = false;
  String? _searchError;

  static const List<String> _quickCategories = [
    'Synthwave Chill',
    'Lo-Fi Beats',
    'Acoustic Sessions',
    'Indie Pop Hits',
    'Electronic Focus',
    'Classical Piano',
  ];

  @override
  void initState() {
    super.initState();
    _performSearch('Lo-Fi Beats');
    widget.playerController.addListener(_onPlayerUpdate);
  }

  @override
  void dispose() {
    _debounce?.cancel();
    _searchCtrl.dispose();
    widget.playerController.removeListener(_onPlayerUpdate);
    super.dispose();
  }

  void _onPlayerUpdate() {
    final err = widget.playerController.errorMessage;
    if (err != null && mounted) {
      ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text(err)));
      widget.playerController.clearError();
    }
  }

  void _onSearchChanged(String value) {
    _debounce?.cancel();
    _debounce = Timer(const Duration(milliseconds: 500), () {
      if (value.trim().isNotEmpty) {
        _performSearch(value);
      }
    });
  }

  Future<void> _performSearch(String query) async {
    setState(() {
      _isSearching = true;
      _searchError = null;
    });
    try {
      final items = await widget.playerController.searchYouTube(query);
      if (mounted) {
        setState(() {
          _results = items;
          _isSearching = false;
        });
      }
    } catch (e) {
      if (mounted) {
        setState(() {
          _isSearching = false;
          _searchError = 'Search failed. Check your internet connection.';
        });
      }
    }
  }

  Future<void> _showImportShareDialog() async {
    final ctrl = TextEditingController();
    await showDialog<void>(
      context: context,
      builder: (ctx) => AlertDialog(
        backgroundColor: AppColors.cardElevated,
        title: const Text('Import Shared Track'),
        content: TextField(
          controller: ctrl,
          decoration: const InputDecoration(hintText: 'Paste spotube://track/... code'),
        ),
        actions: [
          TextButton(onPressed: () => Navigator.pop(ctx), child: const Text('CANCEL')),
          ElevatedButton(
            style: ElevatedButton.styleFrom(backgroundColor: AppColors.spotifyGreen, foregroundColor: Colors.black),
            onPressed: () {
              final track = TrackModel.fromShareToken(ctrl.text);
              Navigator.pop(ctx);
              if (track != null) {
                widget.playerController.playTrack(track);
              } else {
                ScaffoldMessenger.of(context).showSnackBar(
                  const SnackBar(content: Text('Invalid share token.')),
                );
              }
            },
            child: const Text('PLAY'),
          ),
        ],
      ),
    );
    ctrl.dispose();
  }

  void _openFullScreenPlayer() {
    showModalBottomSheet<void>(
      context: context,
      isScrollControlled: true,
      backgroundColor: Colors.transparent,
      builder: (_) => FullScreenPlayerModal(playerController: widget.playerController),
    );
  }

  void _openQueueSheet() {
    showModalBottomSheet<void>(
      context: context,
      backgroundColor: AppColors.cardDark,
      shape: const RoundedRectangleBorder(borderRadius: BorderRadius.vertical(top: Radius.circular(20))),
      builder: (_) => QueueSheet(playerController: widget.playerController),
    );
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        backgroundColor: AppColors.background,
        elevation: 0,
        title: Row(
          children: [
            const Icon(Icons.album_rounded, color: AppColors.spotifyGreen),
            const SizedBox(width: 10),
            Expanded(
              child: Text(
                'Good day, ${widget.authController.displayName}',
                style: const TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
                overflow: TextOverflow.ellipsis,
              ),
            ),
          ],
        ),
        actions: [
          IconButton(
            tooltip: 'Import Shared Track',
            icon: const Icon(Icons.qr_code_scanner_rounded),
            onPressed: _showImportShareDialog,
          ),
          IconButton(
            tooltip: 'Queue',
            icon: const Icon(Icons.queue_music_rounded),
            onPressed: _openQueueSheet,
          ),
          IconButton(
            tooltip: 'Log out',
            icon: const Icon(Icons.logout_rounded),
            onPressed: widget.authController.signOut,
          ),
        ],
      ),
      body: Column(
        children: [
          Padding(
            padding: const EdgeInsets.fromLTRB(16, 8, 16, 8),
            child: TextField(
              controller: _searchCtrl,
              onChanged: _onSearchChanged,
              onSubmitted: _performSearch,
              style: const TextStyle(color: Colors.black),
              decoration: InputDecoration(
                hintText: 'What do you want to listen to?',
                hintStyle: const TextStyle(color: Colors.black54),
                prefixIcon: const Icon(Icons.search, color: Colors.black87),
                suffixIcon: _searchCtrl.text.isNotEmpty
                    ? IconButton(
                        icon: const Icon(Icons.clear, color: Colors.black87),
                        onPressed: () {
                          _searchCtrl.clear();
                          setState(() {});
                        },
                      )
                    : null,
                filled: true,
                fillColor: Colors.white,
                contentPadding: const EdgeInsets.symmetric(vertical: 0),
                border: OutlineInputBorder(borderRadius: BorderRadius.circular(8), borderSide: BorderSide.none),
              ),
            ),
          ),
          SizedBox(
            height: 44,
            child: ListView.separated(
              padding: const EdgeInsets.symmetric(horizontal: 16),
              scrollDirection: Axis.horizontal,
              itemCount: _quickCategories.length,
              separatorBuilder: (_, __) => const SizedBox(width: 8),
              itemBuilder: (context, index) {
                final label = _quickCategories[index];
                return ActionChip(
                  backgroundColor: AppColors.cardElevated,
                  side: BorderSide.none,
                  label: Text(label, style: const TextStyle(color: AppColors.textPrimary, fontSize: 13)),
                  onPressed: () {
                    _searchCtrl.text = label;
                    _performSearch(label);
                  },
                );
              },
            ),
          ),
          const SizedBox(height: 8),
          ListenableBuilder(
            listenable: widget.playerController,
            builder: (context, _) {
              if (!widget.playerController.isResolvingStream) return const SizedBox.shrink();
              return const LinearProgressIndicator(
                color: AppColors.spotifyGreen,
                backgroundColor: AppColors.cardElevated,
                minHeight: 2,
              );
            },
          ),
          Expanded(
            child: _isSearching
                ? const Center(child: CircularProgressIndicator(color: AppColors.spotifyGreen))
                : _searchError != null
                    ? Center(child: Text(_searchError!, style: const TextStyle(color: AppColors.textSecondary)))
                    : ListView.builder(
                        padding: const EdgeInsets.only(bottom: 16),
                        itemCount: _results.length,
                        itemBuilder: (context, index) => TrackTile(
                          track: _results[index],
                          playerController: widget.playerController,
                        ),
                      ),
          ),
          ListenableBuilder(
            listenable: widget.playerController,
            builder: (context, _) {
              final current = widget.playerController.currentTrack;
              if (current == null) return const SizedBox.shrink();
              return MiniPlayerBar(
                track: current,
                playerController: widget.playerController,
                onTap: _openFullScreenPlayer,
              );
            },
          ),
        ],
      ),
    );
  }
}

// ============================================================================
// TRACK LIST TILE
// ============================================================================
class TrackTile extends StatelessWidget {
  final TrackModel track;
  final PlayerController playerController;

  const TrackTile({super.key, required this.track, required this.playerController});

  String _formatDuration(Duration d) {
    final minutes = d.inMinutes;
    final seconds = (d.inSeconds % 60).toString().padLeft(2, '0');
    return '$minutes:$seconds';
  }

  @override
  Widget build(BuildContext context) {
    return ListenableBuilder(
      listenable: playerController,
      builder: (context, _) {
        final isCurrent = playerController.currentTrack?.videoId == track.videoId;
        return ListTile(
          contentPadding: const EdgeInsets.symmetric(horizontal: 16, vertical: 4),
          leading: ClipRRect(
            borderRadius: BorderRadius.circular(6),
            child: Image.network(
              track.thumbnailUrl,
              width: 54,
              height: 54,
              fit: BoxFit.cover,
              errorBuilder: (_, __, ___) => Container(
                width: 54,
                height: 54,
                color: AppColors.cardElevated,
                child: const Icon(Icons.music_note, color: Colors.white54),
              ),
            ),
          ),
          title: Text(
            track.title,
            maxLines: 1,
            overflow: TextOverflow.ellipsis,
            style: TextStyle(
              color: isCurrent ? AppColors.spotifyGreen : AppColors.textPrimary,
              fontWeight: isCurrent ? FontWeight.bold : FontWeight.w500,
              fontSize: 15,
            ),
          ),
          subtitle: Text(
            '${track.author} • ${_formatDuration(track.duration)}',
            maxLines: 1,
            overflow: TextOverflow.ellipsis,
            style: const TextStyle(color: AppColors.textSecondary, fontSize: 13),
          ),
          onTap: () => playerController.playTrack(track),
          trailing: PopupMenuButton<String>(
            icon: const Icon(Icons.more_vert, color: AppColors.textSecondary),
            color: AppColors.cardElevated,
            onSelected: (value) async {
              if (value == 'queue') {
                await playerController.addToQueue(track);
                if (context.mounted) {
                  ScaffoldMessenger.of(context).showSnackBar(
                    SnackBar(content: Text('Added "${track.title}" to queue')),
                  );
                }
              } else if (value == 'share') {
                await playerController.shareTrack(track);
              } else if (value == 'copy') {
                await Clipboard.setData(ClipboardData(text: track.toShareToken()));
                if (context.mounted) {
                  ScaffoldMessenger.of(context).showSnackBar(
                    const SnackBar(content: Text('Share code copied to clipboard')),
                  );
                }
              }
            },
            itemBuilder: (_) => const [
              PopupMenuItem(
                value: 'queue',
                child: Row(children: [Icon(Icons.playlist_add, size: 20), SizedBox(width: 10), Text('Add to Queue')]),
              ),
              PopupMenuItem(
                value: 'share',
                child: Row(children: [Icon(Icons.share, size: 20), SizedBox(width: 10), Text('Share Track')]),
              ),
              PopupMenuItem(
                value: 'copy',
                child: Row(children: [Icon(Icons.copy, size: 20), SizedBox(width: 10), Text('Copy Share Token')]),
              ),
            ],
          ),
        );
      },
    );
  }
}

// ============================================================================
// PERSISTENT MINI-PLAYER BAR
// ============================================================================
class MiniPlayerBar extends StatelessWidget {
  final TrackModel track;
  final PlayerController playerController;
  final VoidCallback onTap;

  const MiniPlayerBar({
    super.key,
    required this.track,
    required this.playerController,
    required this.onTap,
  });

  @override
  Widget build(BuildContext context) {
    final player = playerController.player;
    return GestureDetector(
      onTap: onTap,
      child: Container(
        margin: const EdgeInsets.fromLTRB(8, 0, 8, 8),
        decoration: BoxDecoration(color: AppColors.cardElevated, borderRadius: BorderRadius.circular(10)),
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            Padding(
              padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 8),
              child: Row(
                children: [
                  ClipRRect(
                    borderRadius: BorderRadius.circular(6),
                    child: Image.network(
                      track.thumbnailUrl,
                      width: 44,
                      height: 44,
                      fit: BoxFit.cover,
                      errorBuilder: (_, __, ___) => Container(
                        width: 44,
                        height: 44,
                        color: AppColors.cardDark,
                        child: const Icon(Icons.music_note, size: 20),
                      ),
                    ),
                  ),
                  const SizedBox(width: 12),
                  Expanded(
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        Text(
                          track.title,
                          maxLines: 1,
                          overflow: TextOverflow.ellipsis,
                          style: const TextStyle(fontWeight: FontWeight.w600, fontSize: 14),
                        ),
                        const SizedBox(height: 2),
                        Text(
                          track.author,
                          maxLines: 1,
                          overflow: TextOverflow.ellipsis,
                          style: const TextStyle(color: AppColors.textSecondary, fontSize: 12),
                        ),
                      ],
                    ),
                  ),
                  IconButton(
                    icon: Icon(
                      player.playing ? Icons.pause_rounded : Icons.play_arrow_rounded,
                      size: 30,
                      color: Colors.white,
                    ),
                    onPressed: playerController.togglePlayPause,
                  ),
                  IconButton(
                    icon: const Icon(Icons.skip_next_rounded, size: 28, color: Colors.white),
                    onPressed: player.hasNext ? () => player.seekToNext() : null,
                  ),
                ],
              ),
            ),
            StreamBuilder<PositionData>(
              stream: playerController.positionDataStream,
              builder: (context, snapshot) {
                final pos = snapshot.data?.position ?? Duration.zero;
                final total = snapshot.data?.duration ?? track.duration;
                final maxMs = total.inMilliseconds > 0 ? total.inMilliseconds.toDouble() : 1.0;
                final progress = (pos.inMilliseconds / maxMs).clamp(0.0, 1.0);
                return ClipRRect(
                  borderRadius: const BorderRadius.vertical(bottom: Radius.circular(10)),
                  child: LinearProgressIndicator(
                    value: progress,
                    minHeight: 2.5,
                    backgroundColor: Colors.white12,
                    valueColor: const AlwaysStoppedAnimation<Color>(AppColors.spotifyGreen),
                  ),
                );
              },
            ),
          ],
        ),
      ),
    );
  }
}

// ============================================================================
// FULL-SCREEN PLAYER MODAL
// ============================================================================
class FullScreenPlayerModal extends StatelessWidget {
  final PlayerController playerController;

  const FullScreenPlayerModal({super.key, required this.playerController});

  String _formatDuration(Duration d) {
    final minutes = d.inMinutes;
    final seconds = (d.inSeconds % 60).toString().padLeft(2, '0');
    return '$minutes:$seconds';
  }

  @override
  Widget build(BuildContext context) {
    return ListenableBuilder(
      listenable: playerController,
      builder: (context, _) {
        final track = playerController.currentTrack;
        final player = playerController.player;
        if (track == null) return const SizedBox.shrink();

        return Container(
          height: MediaQuery.of(context).size.height * 0.92,
          decoration: const BoxDecoration(
            gradient: LinearGradient(
              begin: Alignment.topCenter,
              end: Alignment.bottomCenter,
              colors: [Color(0xFF2C3E35), AppColors.background, AppColors.background],
            ),
            borderRadius: BorderRadius.vertical(top: Radius.circular(24)),
          ),
          padding: const EdgeInsets.symmetric(horizontal: 24, vertical: 16),
          child: Column(
            children: [
              Row(
                mainAxisAlignment: MainAxisAlignment.spaceBetween,
                children: [
                  IconButton(
                    icon: const Icon(Icons.keyboard_arrow_down_rounded, size: 32),
                    onPressed: () => Navigator.pop(context),
                  ),
                  const Text(
                    'NOW PLAYING',
                    style: TextStyle(fontSize: 12, fontWeight: FontWeight.bold, letterSpacing: 1.4, color: AppColors.textSecondary),
                  ),
                  IconButton(
                    icon: const Icon(Icons.share_outlined),
                    onPressed: () => playerController.shareTrack(track),
                  ),
                ],
              ),
              const Spacer(),
              AspectRatio(
                aspectRatio: 1,
                child: Container(
                  decoration: BoxDecoration(
                    borderRadius: BorderRadius.circular(14),
                    boxShadow: const [BoxShadow(color: Colors.black54, blurRadius: 24, offset: Offset(0, 12))],
                  ),
                  child: ClipRRect(
                    borderRadius: BorderRadius.circular(14),
                    child: Image.network(
                      track.thumbnailUrl,
                      fit: BoxFit.cover,
                      errorBuilder: (_, __, ___) => Container(
                        color: AppColors.cardElevated,
                        child: const Icon(Icons.music_note, size: 96),
                      ),
                    ),
                  ),
                ),
              ),
                            const Spacer(),
              Row(
                children: [
                  Expanded(
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        Text(
                          track.title,
                          maxLines: 1,
                          overflow: TextOverflow.ellipsis,
                          style: const TextStyle(fontSize: 22, fontWeight: FontWeight.bold),
                        ),
                        const SizedBox(height: 4),
                        Text(
                          track.author,
                          maxLines: 1,
                          overflow: TextOverflow.ellipsis,
                          style: const TextStyle(fontSize: 16, color: AppColors.textSecondary),
                        ),
                      ],
                    ),
                  ),
                  IconButton(
                    tooltip: 'Copy Share Code',
                    icon: const Icon(Icons.copy_rounded),
                    onPressed: () async {
                      await Clipboard.setData(ClipboardData(text: track.toShareToken()));
                      if (context.mounted) {
                        ScaffoldMessenger.of(context).showSnackBar(
                          const SnackBar(content: Text('Share code copied to clipboard!')),
                        );
                      }
                    },
                  ),
                ],
              ),
              const SizedBox(height: 16),
              StreamBuilder<PositionData>(
                stream: playerController.positionDataStream,
                builder: (context, snapshot) {
                  final position = snapshot.data?.position ?? Duration.zero;
                  final buffered = snapshot.data?.bufferedPosition ?? Duration.zero;
                  final total = snapshot.data?.duration ?? track.duration;
                  final maxMs = total.inMilliseconds > 0 ? total.inMilliseconds.toDouble() : 1.0;
                  final currentMs = position.inMilliseconds.toDouble().clamp(0.0, maxMs);
                  final bufferedMs = buffered.inMilliseconds.toDouble().clamp(0.0, maxMs);

                  return Column(
                    children: [
                      Slider(
                        min: 0.0,
                        max: maxMs,
                        value: currentMs,
                        secondaryTrackValue: bufferedMs,
                        secondaryActiveColor: Colors.white38,
                        onChanged: (val) => player.seek(Duration(milliseconds: val.round())),
                      ),
                      Padding(
                        padding: const EdgeInsets.symmetric(horizontal: 12),
                        child: Row(
                          mainAxisAlignment: MainAxisAlignment.spaceBetween,
                          children: [
                            Text(_formatDuration(position), style: const TextStyle(fontSize: 12, color: AppColors.textSecondary)),
                            Text(_formatDuration(total), style: const TextStyle(fontSize: 12, color: AppColors.textSecondary)),
                          ],
                        ),
                      ),
                    ],
                  );
                },
              ),
              const SizedBox(height: 12),
              Row(
                mainAxisAlignment: MainAxisAlignment.spaceEvenly,
                children: [
                  IconButton(
                    icon: Icon(
                      Icons.shuffle_rounded,
                      color: player.shuffleModeEnabled ? AppColors.spotifyGreen : AppColors.textSecondary,
                    ),
                    onPressed: playerController.toggleShuffle,
                  ),
                  IconButton(
                    iconSize: 38,
                    icon: const Icon(Icons.skip_previous_rounded),
                    onPressed: player.hasPrevious ? () => player.seekToPrevious() : () => player.seek(Duration.zero),
                  ),
                  Container(
                    width: 68,
                    height: 68,
                    decoration: const BoxDecoration(color: AppColors.spotifyGreen, shape: BoxShape.circle),
                    child: IconButton(
                      iconSize: 38,
                      color: Colors.black,
                      icon: Icon(player.playing ? Icons.pause_rounded : Icons.play_arrow_rounded),
                      onPressed: playerController.togglePlayPause,
                    ),
                  ),
                  IconButton(
                    iconSize: 38,
                    icon: const Icon(Icons.skip_next_rounded),
                    onPressed: player.hasNext ? () => player.seekToNext() : null,
                  ),
                  IconButton(
                    icon: Icon(
                      player.loopMode == LoopMode.one ? Icons.repeat_one_rounded : Icons.repeat_rounded,
                      color: player.loopMode != LoopMode.off ? AppColors.spotifyGreen : AppColors.textSecondary,
                    ),
                    onPressed: playerController.cycleLoopMode,
                  ),
                ],
              ),
              const SizedBox(height: 16),
              Row(
                mainAxisAlignment: MainAxisAlignment.spaceBetween,
                children: [
                  Text(
                    switch (player.loopMode) {
                      LoopMode.off => 'Repeat: Off',
                      LoopMode.all => 'Repeat: Queue',
                      LoopMode.one => 'Repeat: Track',
                    },
                    style: const TextStyle(color: AppColors.textSecondary, fontSize: 12),
                  ),
                  TextButton.icon(
                    onPressed: () {
                      showModalBottomSheet<void>(
                        context: context,
                        backgroundColor: AppColors.cardDark,
                        shape: const RoundedRectangleBorder(borderRadius: BorderRadius.vertical(top: Radius.circular(20))),
                        builder: (_) => QueueSheet(playerController: playerController),
                      );
                    },
                    icon: const Icon(Icons.queue_music_rounded, color: AppColors.textPrimary),
                    label: Text('Queue (${playerController.queue.length})', style: const TextStyle(color: AppColors.textPrimary)),
                  ),
                ],
              ),
            ],
          ),
        );
      },
    );
  }
}

// ============================================================================
// QUEUE VIEWER BOTTOM SHEET
// ============================================================================
class QueueSheet extends StatelessWidget {
  final PlayerController playerController;

  const QueueSheet({super.key, required this.playerController});

  @override
  Widget build(BuildContext context) {
    return ListenableBuilder(
      listenable: playerController,
      builder: (context, _) {
        final queue = playerController.queue;
        final currentIndex = playerController.player.currentIndex;

        return SafeArea(
          child: Padding(
            padding: const EdgeInsets.symmetric(vertical: 16),
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                const Padding(
                  padding: EdgeInsets.symmetric(horizontal: 20, vertical: 8),
                  child: Row(
                    children: [
                      Icon(Icons.queue_music, color: AppColors.spotifyGreen),
                      SizedBox(width: 10),
                      Text('Playback Queue', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
                    ],
                  ),
                ),
                const Divider(color: Colors.white12),
                if (queue.isEmpty)
                  const Padding(
                    padding: EdgeInsets.all(32),
                    child: Text(
                      'Your queue is empty. Tap any song to start listening.',
                      style: TextStyle(color: AppColors.textSecondary),
                    ),
                  )
                else
                  Flexible(
                    child: ListView.builder(
                      shrinkWrap: true,
                      itemCount: queue.length,
                      itemBuilder: (context, index) {
                        final item = queue[index];
                        final isPlaying = index == currentIndex;
                        return ListTile(
                          leading: Icon(
                            isPlaying ? Icons.equalizer_rounded : Icons.music_note_rounded,
                            color: isPlaying ? AppColors.spotifyGreen : AppColors.textSecondary,
                          ),
                          title: Text(
                            item.title,
                            maxLines: 1,
                            overflow: TextOverflow.ellipsis,
                            style: TextStyle(
                              color: isPlaying ? AppColors.spotifyGreen : AppColors.textPrimary,
                              fontWeight: isPlaying ? FontWeight.bold : FontWeight.normal,
                            ),
                          ),
                          subtitle: Text(
                            item.author,
                            maxLines: 1,
                            overflow: TextOverflow.ellipsis,
                            style: const TextStyle(color: AppColors.textSecondary, fontSize: 12),
                          ),
                          trailing: IconButton(
                            icon: const Icon(Icons.remove_circle_outline, color: AppColors.textSecondary),
                            onPressed: () => playerController.removeFromQueue(index),
                          ),
                          onTap: () {
                            playerController.playFromQueueIndex(index);
                            Navigator.pop(context);
                          },
                        );
                      },
                    ),
                  ),
              ],
            ),
          ),
        );
      },
    );
  }
}
